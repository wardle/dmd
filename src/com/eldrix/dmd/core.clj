(ns com.eldrix.dmd.core
  (:require [clojure.core.async :as a]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.dmd.import :as dim]
            [com.eldrix.dmd.store :as st]
            [clojure.string :as str]
            [com.eldrix.trud.zip :as zipfile])
  (:import (java.time.format DateTimeFormatter)))

(defn import-dmd
  "Creates a new dm+d filestore at `filename` from the directory specified."
  [filename dir]
  (let [ch (a/chan)]
    (a/thread (dim/stream-dmd dir ch :ordered? true))
    (st/create-store filename ch)))

(defn install-latest
  "Create a versioned dm+d file-based database by downloading the latest dm+d
  distribution automatically from NHS Digital's TRUD service."
  [api-key-file cache-dir]
  (let [api-key (str/trim-newline (slurp api-key-file))
        release (com.eldrix.dmd.download/download-release api-key cache-dir)
        _ (log/info "Downloaded dm+d release " release)
        unzipped (zipfile/unzip (:archiveFilePath release))
        filename (str "dmd-" (.format (DateTimeFormatter/ISO_LOCAL_DATE) (:releaseDate release )) ".db")]
    (log/info "Creating dm+d file-based database :" filename)
    (import-dmd filename (.toFile unzipped))
    (zipfile/delete-paths [unzipped])))

(comment
  (import-dmd "dmd.db" "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001")
  (install-latest "/Users/mark/Dev/trud/api-key.txt" "/var/tmp/trud")


  (def store (st/open-dmd-store "dmd.db"))
  (.close store)
  (def vtm (st/fetch-product store 108537001))
  vtm
  (st/vmps store vtm)
  (st/vmpps store vtm)
  (st/amps store vtm)
  (map :NM (map (partial st/fetch-product store) (st/ampps store vtm)))
  )

