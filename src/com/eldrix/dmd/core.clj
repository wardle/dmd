(ns com.eldrix.dmd.core
  (:require [clojure.core.async :as a]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.dmd.download :as dl]
            [com.eldrix.dmd.import :as dim]
            [com.eldrix.dmd.store4 :as st4]
            [clojure.string :as str]
            [com.eldrix.trud.core :as trud])
  (:import (java.time.format DateTimeFormatter)))

(defn install-from-dirs
  "Creates a new dm+d filestore at `filename` from the directories specified."
  [filename dirs & {:keys [_batch-size] :as opts}]
  (when (= 0 (count dirs)) (throw (ex-info "no directories specified" {:filename filename :dirs dirs})))
  (let [release-date (last (sort (remove nil? (map #(:release-date (dim/get-release-metadata %)) dirs))))
        {:keys [errors]} (st4/create-store filename dirs (assoc opts :release-date release-date))]
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
         filename' (if filename filename (str "dmd-" (.format DateTimeFormatter/ISO_LOCAL_DATE (:releaseDate (first releases))) ".db"))]
     (log/info "Creating dm+d file-based database :" filename')
     (install-from-dirs filename' (map #(.toFile ^java.nio.file.Path %) unzipped))
     ;(zipfile/delete-paths unzipped)
     (log/info "Created dm+d file-based database :" filename'))))

(defn install-latest
  "Convenience function to install latest release using a filename based on its release date. 
  For more control, use `install-release`."
  [api-key-file cache-dir]
  (install-release api-key-file cache-dir))

(defn open-store [filename]
  (st4/open-store filename))

(defn fetch-release-date [store]
  (st4/fetch-release-date store))

(defn fetch-product [store product-id]
  (st4/fetch-product store product-id))

(defn fetch-product-by-exact-name [conn nm]
  (st4/fetch-product-by-exact-name conn nm))

(defn fetch-lookup [conn lookup-kind]
  (st4/fetch-all-lookup conn lookup-kind))

(defn ^:deprecated vmps-from-atc
  "DEPRECATED: use [[vpids-from-atc]] instead."
  [conn atc]
  (st4/vmps-from-atc conn atc))

(defn vpids-from-atc
  [conn atc]
  (st4/vpids-from-atc conn atc))

(defn ^:deprecated products-from-atc
  "Returns a sequence of products matching the ATC code.
  Parameters:
  - conn          :
  - atc           : atc code / prefix
  - product-types : a set of product types (e.g. #{:VTM :VMP :AMP :VMPP :AMPP}).

  By default only VTM VMP and AMP products will be returned."
  ([conn atc]
   (products-from-atc conn atc #{:VTM :VMP :AMP}))
  ([conn atc product-types]
   (map #(st4/fetch-product conn %) (st4/product-ids-from-atc conn atc product-types))))

(defn ^:deprecated atc->snomed-ecl
  "Create a SNOMED CT ECL expression from the ATC pattern specified, returning
  an expression that will return VTMs, VMPs and AMPs.
  Prefer to use [[atc->products-for-ecl]] and the SNOMED CT drug extension
  to optimise the creation of the appropriate ECL expression."
  [conn atc]
  (st4/atc->ecl conn atc))

(defn atc->products-for-ecl
  "Return a map of products that can be used to build a more complete SNOMED CT
  ECL expression that will include all matching UK products. We have to do it
  this way because TF products are not included in the UK dm+d distribution."
  [conn atc]
  (st4/atc->products-for-ecl conn atc))

(defn vmps-for-product [conn id]
  (->> (st4/vpids conn id)
       (map #(st4/fetch-vmp conn %))))

(defn amps-for-product [conn id]
  (->> (st4/apids conn id)
       (map #(st4/fetch-amp conn %))))

(defn vtms-for-product [conn id]
  (->> (st4/vtmids conn id)
       (map #(st4/fetch-vtm conn %))))

(defn atc-for-product [conn id]
  (st4/atc-code conn id))

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


