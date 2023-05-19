(ns com.eldrix.dmd.core
  (:require [clojure.core.async :as a]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.dmd.download :as dl]
            [com.eldrix.dmd.import :as dim]
            [com.eldrix.dmd.store2 :as st2]
            [clojure.string :as str]
            [com.eldrix.trud.core :as trud])
  (:import (java.time.format DateTimeFormatter)
           (com.eldrix.dmd.store2 DmdStore)
           (java.time LocalDate)))

(defn install-from-dirs
  "Creates a new dm+d filestore at `filename` from the directories specified."
  [filename dirs & {:keys [_batch-size] :as opts}]
  (when (= 0 (count dirs)) (throw (ex-info "no directories specified" {:filename filename :dirs dirs})))
  (let [ch (a/chan)
        release-date (last (sort (remove nil? (map #(:release-date (dim/get-release-metadata %)) dirs))))]
    (a/thread
      (doseq [dir dirs]
        (dim/stream-dmd dir ch :close? false))
      (a/close! ch))
    (st2/create-store filename ch (assoc opts :release-date release-date))))

(defn print-available-releases
  [api-key]
  (dl/print-available-releases api-key))

(defn install-release
  "Create a versioned dm+d file-based database by downloading the dm+d
  distributions automatically from NHS Digital's TRUD service."
  ([api-key-file cache-dir] (install-release api-key-file cache-dir nil nil))
  ([api-key-file cache-dir filename] (install-release api-key-file cache-dir filename nil))
  ([api-key-file cache-dir filename release-date]
   (let [api-key (str/trim-newline (slurp api-key-file))
         releases (dl/download-release api-key cache-dir release-date)
         _ (log/info "Downloaded dm+d releases " releases)
         unzipped (doall (map #(trud/unzip-nested (:archiveFilePath %)) releases))
         filename' (if filename filename (str "dmd-" (.format (DateTimeFormatter/ISO_LOCAL_DATE) (:releaseDate (first releases))) ".db"))]
     (log/info "Creating dm+d file-based database :" filename')
     (install-from-dirs filename' (map #(.toFile %) unzipped))
     ;(zipfile/delete-paths unzipped)
     (log/info "Created dm+d file-based database :" filename'))))

(defn install-latest
  [api-key-file cache-dir]
  (install-release api-key-file cache-dir))

(defn open-store [filename]
  (st2/open-store filename))

(defn fetch-release-date [^DmdStore store]
  (st2/fetch-release-date store))

(defn fetch-product [^DmdStore store product-id]
  (st2/fetch-product store product-id))

(defn fetch-product-by-exact-name [^DmdStore store nm]
  (when-let [id (st2/product-by-exact-name store nm)]
    (st2/fetch-product store id)))

(defn fetch-lookup [^DmdStore store lookup-kind]
  (st2/fetch-lookup store lookup-kind))

(defn vmps-from-atc [^DmdStore store atc]
  (let [atc' (if (string? atc) (re-pattern atc) atc)]
    (st2/results-for-eids store (st2/vmp-eids-from-atc store atc'))))

(defn products-from-atc
  "Returns a sequence of products matching the ATC code.
  Parameters:
  - store         : DmdStore
  - atc           : atc regexp
  - product-types : a set of product types (e.g. #{:VTM :VMP :AMP :VMPP :AMPP}).

  By default only VTM VMP and AMP products will be returned."
  ([^DmdStore store atc]
   (products-from-atc store atc #{:VTM :VMP :AMP}))
  ([^DmdStore store atc product-types]
   (let [atc' (if (string? atc) (re-pattern atc) atc)]
     (st2/results-for-eids store (st2/product-eids-from-atc store atc' product-types)))))

(defn atc->snomed-ecl
  "Create a SNOMED CT ECL expression from the ATC pattern specified, returning
  an expression that will return VTMs, VMPs and AMPs. "
  [^DmdStore store atc]
  (st2/atc->ecl store (if (string? atc) (re-pattern atc) atc)))

(defn atc->products-for-ecl
  "Return a map of products that can be used to build a more complete SNOMED CT
  ECL expression that will include all matching UK products. We have to do it
  this way because TF products are not included in the UK dm+d distribution."
  [^DmdStore store atc]
  (st2/atc->products-for-ecl store atc))

(defn vmps-for-product [^DmdStore store id]
  (when-let [product (fetch-product store id)]
    (st2/results-for-eids store (st2/vmps store product))))

(defn amps-for-product [^DmdStore store id]
  (when-let [product (fetch-product store id)]
    (st2/results-for-eids store (st2/amps store product))))

(defn vtms-for-product [^DmdStore store id]
  (when-let [product (fetch-product store id)]
    (st2/results-for-eids store (st2/vtms store product))))

(defn atc-for-product [^DmdStore store id]
  (when-let [product (st2/fetch-product store id)]
    (st2/atc-code store product)))

(comment
  (install-latest "/Users/mark/Dev/trud/api-key.txt" "/var/tmp/trud")

  (def store (open-store "dmd-2023-03-27.db"))
  (.close store)
  (def vtm (fetch-product store 774557006))
  (fetch-product-by-exact-name store "Amlodipine")
  vtm
  (vmps-for-product store 774557006)
  (time (atc-for-product store 20428411000001100))

  (dim/dmd-file-seq "/var/folders/w_/s108lpdd1bn84sntjbghwz3w0000gn/T/trud16249114005046941653")

  (def ch (a/chan))
  (a/thread (dim/stream-dmd "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001" ch))
  (a/thread (dim/stream-dmd "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001" ch :include #{:VTM}))
  (a/thread (dim/stream-dmd "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001" ch :include #{:VMP}))
  (a/<!! ch))


