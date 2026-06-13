(ns com.eldrix.dmd.core
  "The public API for the UK NHS Dictionary of medicines and devices (dm+d).

  A dm+d service is a file-based SQLite database, installed from a published
  distribution (see [[install-release]] and [[install-from-dirs]]) and then
  opened read-only (see [[open-store]] and [[close]]).

  All query functions take an open store as their first parameter, `conn`.
  Naming conventions:
  - `fetch-x`          : return a single entity by identifier, as a map of
                         namespaced keys (e.g. :VMP/NM) with relationships
                         resolved
  - `xids-for-product` : identifiers of related products, traversing the
                         dm+d hierarchy from any product
  - `xs-for-product`   : as above, but returning extended product data
  - `x-from-y`         : dm+d identifiers from an external code system,
                         such as ATC or GTIN
  - `plan-x`           : a reducible over all rows of a kind, for streaming
                         iteration without realising all rows in memory"
  (:require [clojure.core.async :as a]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.dmd.download :as dl]
            [com.eldrix.dmd.import :as dim]
            [com.eldrix.dmd.store :as st]
            [clojure.string :as str]
            [com.eldrix.trud.core :as trud])
  (:import (java.time.format DateTimeFormatter)))

(defn install-from-dirs
  "Creates a new dm+d filestore at `filename` from the directories specified.
  Options:
  - :batch-size - number of components per write batch (default 50000)
  - :trud       - source TRUD release information, stored for provenance;
                  supplied automatically when installed via [[install-release]]"
  [filename dirs & {:keys [_batch-size _trud] :as opts}]
  (when (= 0 (count dirs)) (throw (ex-info "no directories specified" {:filename filename :dirs dirs})))
  (let [release-date (last (sort (remove nil? (map #(:release-date (dim/get-release-metadata %)) dirs))))
        {:keys [errors]} (st/create-store filename dirs (assoc opts :release-date release-date))]
    (doseq [err errors]
      (log/error "error during import: " err))))

(defn print-available-releases
  "Print dm+d releases available from NHS Digital's TRUD service. Note: takes
  the API key itself, unlike [[install-release]] which takes the name of a
  file containing the key."
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

(defn close
  "Close a store opened by [[open-store]], releasing pooled connections."
  [store]
  (st/close store))

(defn fetch-release-date
  "Returns the date (java.time.LocalDate) of the dm+d release in the store."
  [conn]
  (st/fetch-release-date conn))

(defn status
  "Returns a structured description of an open dm+d store, including store
  schema version, creation date, dm+d release date, source TRUD release
  information and file inventory when available, and entity counts."
  [conn]
  (st/status conn))

(defn fetch-product
  "Returns extended information about the product with the given identifier,
  of whatever product type, as a map of namespaced keys (e.g. :VMP/NM) with
  relationships and lookups resolved; nil if not found."
  [conn product-id]
  (st/fetch-product conn product-id))

(defn product-type
  "Returns a keyword :VTM :VMP :AMP :VMPP or :AMPP for the product with the
  given identifier, or nil if it is not a current product identifier."
  [conn id]
  (st/product-type conn id))

(defn fetch-product-by-exact-name
  "Returns a single product with the given exact name, checking each product
  type in turn; nil if no product matches. For tokenized prefix matching,
  use [[search]]."
  [conn nm]
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

(defn fetch-lookup
  "Returns lookup table entries of the given kind (e.g. :LEGAL_CATEGORY); see
  [[lookup-types]]. With two arguments, returns all entries; with a code,
  returns the single matching entry, or nil."
  ([conn lookup-kind]
   (st/fetch-all-lookup conn lookup-kind))
  ([conn lookup-kind code]
   (st/fetch-lookup conn lookup-kind code)))

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

(defn isids-for-vtm
  "Returns ingredient (ISID) identifiers for the given VTM."
  [conn vtmid]
  (st/isids-for-vtm conn vtmid))

(defn vtmids-for-ingredient
  "Returns VTM identifiers for the given ingredient."
  [conn isid]
  (st/vtmids-for-ingredient conn isid))

(defn fetch-ingredient
  "Returns the ingredient with the given ISID."
  [conn isid]
  (st/fetch-ingredient conn isid))

(defn gtins-for-appid
  "Returns GTINs (Global Trade Item Numbers) for the given AMPP, as a vector
  of strings. By default, only GTINs valid on the current date are returned:
  a GTIN is valid from its start date to its end date inclusive, so expired
  entries, such as for packs no longer in circulation, are omitted.
  Options:
  - :on-date         - java.time.LocalDate on which to assess validity;
                       default, today
  - :include-expired - when true, return all GTINs irrespective of their
                       period of validity"
  [conn appid & {:keys [_on-date _include-expired] :as opts}]
  (st/gtins-for-appid conn appid opts))

(defn appids-from-gtin
  "Returns AMPP identifiers for the given GTIN (Global Trade Item Number),
  which may be a string or a number. Usually a single identifier, but the
  data model permits more than one. By default, only assignments valid on
  the current date are returned: a GTIN is valid from its start date to its
  end date inclusive, so expired entries are omitted.
  Options:
  - :on-date         - java.time.LocalDate on which to assess validity;
                       default, today
  - :include-expired - when true, return all AMPPs irrespective of their
                       period of validity"
  [conn gtin & {:keys [_on-date _include-expired] :as opts}]
  (st/appids-from-gtin conn gtin opts))

(defn plan-products
  "Returns a reducible over all rows of the given product type (:VTM :VMP
  :AMP :VMPP or :AMPP), for streaming iteration; each row is a `next.jdbc`
  row abstraction with columns accessible by keyword."
  [conn kind]
  (st/plan-products conn kind))

(defn plan-ingredients
  "Returns a reducible over all rows of the INGREDIENT table, for streaming
  iteration; each row is a `next.jdbc` row abstraction with columns
  accessible by keyword, e.g. :ISID and :NM."
  [conn]
  (st/plan-ingredients conn))

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

(defn vpids-for-product
  "Returns VMP identifiers for the given product, as a vector."
  [conn product-id]
  (st/vpids conn product-id))

(defn vtmids-for-product
  "Returns VTM identifiers for the given product, as a set."
  [conn product-id]
  (st/vtmids conn product-id))

(defn apids-for-product
  "Returns AMP identifiers for the given product, as a vector."
  [conn product-id]
  (st/apids conn product-id))

(defn vppids-for-product
  "Returns VMPP identifiers for the given product, as a vector."
  [conn product-id]
  (st/vppids conn product-id))

(defn appids-for-product
  "Returns AMPP identifiers for the given product, as a vector."
  [conn product-id]
  (st/appids conn product-id))

(defn subsumes?
  "Returns true if product `a` subsumes product `b`: that is, `a` is an
  ancestor of `b` in the dm+d product hierarchy, in which an AMPP is both an
  AMP and a VMPP, an AMP and a VMPP are each a VMP, and a VMP is a VTM.
  Strict: a product does not subsume itself, so equal identifiers return
  false, as does any identifier that is not a current product identifier;
  equivalence is left to the caller."
  [conn a b]
  (st/subsumes? conn a b))

(defn vtms-for-product
  "Returns VTMs for the given product, as extended product maps; see
  [[vtmids-for-product]] for identifiers only."
  [conn product-id]
  (->> (st/vtmids conn product-id)
       (map #(st/fetch-vtm conn %))))

(defn vmps-for-product
  "Returns VMPs for the given product, as extended product maps; see
  [[vpids-for-product]] for identifiers only."
  [conn product-id]
  (->> (st/vpids conn product-id)
       (map #(st/fetch-vmp conn %))))

(defn amps-for-product
  "Returns AMPs for the given product, as extended product maps; see
  [[apids-for-product]] for identifiers only."
  [conn product-id]
  (->> (st/apids conn product-id)
       (map #(st/fetch-amp conn %))))

(defn vmpps-for-product
  "Returns VMPPs for the given product, as extended product maps; see
  [[vppids-for-product]] for identifiers only."
  [conn product-id]
  (->> (st/vppids conn product-id)
       (map #(st/fetch-vmpp conn %))))

(defn ampps-for-product
  "Returns AMPPs for the given product, as extended product maps; see
  [[appids-for-product]] for identifiers only."
  [conn product-id]
  (->> (st/appids conn product-id)
       (map #(st/fetch-ampp conn %))))

(defn atc-for-product
  "Returns the ATC code for the given product. When a product maps to more
  than one ATC code, such as a VTM whose VMPs carry different codes, one is
  returned arbitrarily."
  [conn product-id]
  (st/atc-code conn product-id))

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


