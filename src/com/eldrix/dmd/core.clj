(ns com.eldrix.dmd.core
  (:require [clojure.core.async :as a]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.dmd.download :as dl]
            [com.eldrix.dmd.import :as dim]
            [com.eldrix.dmd.store :as st]
            [clojure.string :as str]
            [com.eldrix.trud.zip :as zipfile]
            [com.eldrix.dmd.store :as store])
  (:import (java.time.format DateTimeFormatter)))

(defn install-from-dirs
  "Creates a new dm+d filestore at `filename` from the directories specified."
  [filename dirs]
  (when (= 0 (count dirs)) (throw (ex-info "no directories specified" {:filename filename :dirs dirs})))
  (let [ch (a/chan)]
    (a/thread
      (doseq [dir dirs]
        (dim/stream-dmd dir ch :close? false))
      (a/close! ch))
    (st/create-store filename ch)))

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
         unzipped (doall (map #(zipfile/unzip-nested (:archiveFilePath %)) releases))
         filename' (if filename filename (str "dmd-" (.format (DateTimeFormatter/ISO_LOCAL_DATE) (:releaseDate (first releases))) ".db"))]
     (log/info "Creating dm+d file-based database :" filename')
     (install-from-dirs filename' (map #(.toFile %) unzipped))
     ;(zipfile/delete-paths unzipped)
     (log/info "Created dm+d file-based database :" filename'))))

(defn install-latest
  [api-key-file cache-dir]
  (install-release api-key-file cache-dir))

(defn open-store [filename]
  (st/open-dmd-store filename))

(comment
  (import-dmd "dmd.db" "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001")
  (install-latest "/Users/mark/Dev/trud/api-key.txt" "/var/tmp/trud")

  (def store (open-store "dmd-2021-07-05.db"))
  (.close store)
  (def vtm (st/fetch-product store 108537001))
  vtm
  (st/vmps store vtm)
  (st/make-extended-vmp store (st/fetch-product store 39828011000001104))
  (st/bnf-for-product store 39828011000001104)
  (st/vmpps store vtm)
  (st/amps store vtm)
  (map :NM (map (partial st/fetch-product store) (st/ampps store vtm)))

  (dim/dmd-file-seq "/var/folders/w_/s108lpdd1bn84sntjbghwz3w0000gn/T/trud16249114005046941653")
  )

