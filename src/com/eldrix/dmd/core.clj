(ns com.eldrix.dmd.core
  (:require [clojure.core.async :as a]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.dmd.download :as dl]
            [com.eldrix.dmd.import :as dim]
            [com.eldrix.dmd.store :as st]
            [clojure.string :as str]
            [com.eldrix.trud.core :as trud])
  (:import (java.time.format DateTimeFormatter)))

(defn install-from-dirs
  "Creates a new dm+d filestore at `filename` from the directories specified."
  [filename dirs & {:keys [_batch-size] :as opts}]
  (when (= 0 (count dirs)) (throw (ex-info "no directories specified" {:filename filename :dirs dirs})))
  (let [release-date (last (sort (remove nil? (map #(:release-date (dim/get-release-metadata %)) dirs))))
        {:keys [errors]} (st/create-store filename dirs (assoc opts :release-date release-date))]
    (doseq [err errors]
      (log/error "error during import: " err))))

(defn print-available-releases
  [api-key]
  (dl/print-available-releases api-key))

(defn install-release
  "Create a versioned dm+d file-based database by downloading the dm+d
  distributions automatically from NHS Digital's TRUD service.
  Parameters:
  - api-key-file : file containing TRUD API key, anything coercible using 
                   [[clojure.java.io/as-file]]
  - cache-dir    : TRUD cache directory
  - filename     : filename of database to install, can be omitted
  - release-date : date of release to be installed, will use latest if omitted.
                   can be a string in ISO format, or a java.time.LocalDate"
  ([api-key-file cache-dir]
   (install-release api-key-file cache-dir nil nil))
  ([api-key-file cache-dir filename]
   (install-release api-key-file cache-dir filename nil))
  ([api-key-file cache-dir filename release-date]
   (let [api-key (str/trim-newline (slurp api-key-file))
         releases (dl/download-release api-key cache-dir release-date)
         _ (log/info "Downloaded dm+d releases " releases)
         unzipped (doall (map #(trud/unzip-nested (:archiveFilePath %)) releases))
         filename' (if filename filename (str "dmd-" (.format DateTimeFormatter/ISO_LOCAL_DATE (:releaseDate (first releases))) ".db"))
         trud-info (mapv (fn [release]
                           (-> (select-keys release [:itemIdentifier :id :name :releaseDate :archiveFileName])
                               (update :releaseDate str)))
                         releases)]
     (log/info "Creating dm+d file-based database :" filename')
     (install-from-dirs filename' (map #(.toFile ^java.nio.file.Path %) unzipped) :trud trud-info)
     ;(zipfile/delete-paths unzipped)
     (log/info "Created dm+d file-based database :" filename'))))

(defn install-latest
  "Convenience function to install latest release using a filename based on its release date. 
  For more control, use `install-release`."
  [api-key-file cache-dir]
  (install-release api-key-file cache-dir))

(defn open-store
  "Open a dm+d store. Returns what should be regarded as an opaque handle,
  that should be closed using `close`. Currently this is a DataSource but
  this is subject to change. `f` can be anything coercible to a file using
  [[clojure.java.io/as-file]]. Throws an exception if the file does not exist."
  [f]
  (st/open-store f))

(defn sqlite-database?
  "Returns true if `f` is a SQLite 3 database file."
  [f]
  (st/sqlite-database? f))

(defn dmd-database?
  "Returns true if `f` is a dm+d SQLite database created by this library.
  Strict: legacy dm+d files predating the application_id marker return false."
  [f]
  (st/dmd-database? f))

(defn close [st]
  (st/close st))

(defn fetch-release-date [store]
  (st/fetch-release-date store))

(defn status
  "Returns a structured description of an open dm+d store, including store
  schema version, creation date, dm+d release date, source TRUD release
  information and file inventory when available, and entity counts."
  [store]
  (st/status store))

(defn fetch-product [store product-id]
  (st/fetch-product store product-id))

(defn fetch-product-by-exact-name [conn nm]
  (st/fetch-product-by-exact-name conn nm))

(defn search
  "Search product names, returning a sequence of maps of :SEARCH/ID,
  :SEARCH/TYPE and :SEARCH/NM, best matches first. Each token is matched as
  a prefix; multiple tokens must all match.
  Options:
  - :types - product types to include e.g. #{:VMP :AMP}; default, all
  - :limit - maximum number of results; default 100"
  [conn s & {:keys [_types _limit] :as opts}]
  (st/search conn s opts))

(defn fetch-lookup [conn lookup-kind]
  (st/fetch-all-lookup conn lookup-kind))

(def lookup-types
  "Set of lookup types, as keywords, usable with [[fetch-lookup]]."
  st/lookup-types)

(defn fetch-history
  "Returns all history entries in which `id` is the current identifier,
  ordered by start date, including 'self' entries recording the period of
  validity of the current identifier itself."
  [conn id]
  (st/fetch-history conn id))

(defn previous-ids
  "Returns the set of prior identifiers for the given current identifier."
  [conn id]
  (st/previous-ids conn id))

(defn current-ids
  "Returns the set of identifiers in current use for the given (usually
  historic) identifier."
  [conn id]
  (st/current-ids conn id))

(defn vtm-ingredients
  "Returns ingredient (ISID) identifiers for the given VTM."
  [conn vtmid]
  (st/vtm-ingredients conn vtmid))

(defn vtms-for-ingredient
  "Returns VTM identifiers for the given ingredient."
  [conn isid]
  (st/vtms-for-ingredient conn isid))

(defn plan-products
  "Returns a reducible over all rows of the given product type (:VTM :VMP
  :AMP :VMPP or :AMPP), for streaming iteration; each row is a `next.jdbc`
  row abstraction with columns accessible by keyword."
  [conn product-type]
  (st/plan-products conn product-type))

(defn ^:deprecated vmps-from-atc
  "DEPRECATED: use [[vpids-from-atc]] instead."
  [conn atc]
  (st/vmps-from-atc conn atc))

(defn vpids-from-atc
  "Returns a sequence of VPIDs for the ATC code specified. 
  - conn
  - atc - atc code - supports '*' for multiple character wildcard, and '?' for single character wildcard."
  [conn atc]
  (st/vpids-from-atc conn atc))

(defn ^:deprecated products-from-atc
  "Returns a sequence of products matching the ATC code.
  Parameters:
  - conn          :
  - atc           : atc code - supports '*' and '?' for pattern matching as per Lucene
  - product-types : a set of product types (e.g. #{:VTM :VMP :AMP :VMPP :AMPP}).

  By default only VTM VMP and AMP products will be returned."
  ([conn atc]
   (products-from-atc conn atc #{:VTM :VMP :AMP}))
  ([conn atc product-types]
   (map #(st/fetch-product conn %) (st/product-ids-from-atc conn atc product-types))))

(defn ^:deprecated atc->snomed-ecl
  "Create a SNOMED CT ECL expression from the ATC pattern specified, returning
  an expression that will return VTMs, VMPs and AMPs.
  Prefer to use [[atc->products-for-ecl]] and the SNOMED CT drug extension
  to optimise the creation of the appropriate ECL expression."
  [conn atc]
  (st/atc->ecl conn atc))

(defn atc->products-for-ecl
  "Return a map of products that can be used to build a more complete SNOMED CT
  ECL expression that will include all matching UK products. We have to do it
  this way because TF products are not included in the UK dm+d distribution."
  [conn atc]
  (st/atc->products-for-ecl conn atc))

(defn vmps-for-product [conn id]
  (->> (st/vpids conn id)
       (map #(st/fetch-vmp conn %))))

(defn amps-for-product [conn id]
  (->> (st/apids conn id)
       (map #(st/fetch-amp conn %))))

(defn vtms-for-product [conn id]
  (->> (st/vtmids conn id)
       (map #(st/fetch-vtm conn %))))

(defn atc-for-product [conn id]
  (st/atc-code conn id))

(comment
  (install-latest "/Users/mark/Dev/trud/api-key.txt" "/Users/mark/Dev/trud/cache/tmp/trud")
  (def conn (open-store "dmd-2024-01-29.db"))
  ()
  (.close conn)
  (def vtm (fetch-product conn 774557006))
  (fetch-product-by-exact-name conn "Amlodipine")
  vtm
  (vmps-for-product conn 774557006)
  (time (atc-for-product conn 20428411000001100))

  (dim/dmd-file-seq "/var/folders/w_/s108lpdd1bn84sntjbghwz3w0000gn/T/trud16249114005046941653")

  (def ch (a/chan))
  (a/thread (dim/stream-dmd "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001" ch))
  (a/thread (dim/stream-dmd "/Users/mark/Dev/trud/cache" ch :include #{:BNF}))
  (a/thread (dim/stream-dmd "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001" ch :include #{:VMP}))
  (a/<!! ch))


