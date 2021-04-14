(ns com.eldrix.dmd.core
  (:require [clojure.core.async :as a]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.dmd.download :as dl]
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

(defn install-release
  "Create a versioned dm+d file-based database by downloading the dm+d
  distribution automatically from NHS Digital's TRUD service."
  ([api-key-file cache-dir filename] (install-release api-key-file cache-dir filename nil))
  ([api-key-file cache-dir filename release-date]
   (let [api-key (str/trim-newline (slurp api-key-file))
         release (dl/download-release api-key cache-dir release-date)
         _ (log/info "Downloaded dm+d release " release)
         unzipped (zipfile/unzip (:archiveFilePath release))
         filename' (if filename filename (str "dmd-" (.format (DateTimeFormatter/ISO_LOCAL_DATE) (:releaseDate release)) ".db"))]
     (log/info "Creating dm+d file-based database :" filename')
     (import-dmd filename' (.toFile unzipped))
     (zipfile/delete-paths [unzipped])
     (log/info "Created dm+d file-based database :" filename'))))

(def cli-options
  [[nil "--api-key PATH" "Path to file containing TRUD API key"]
   [nil "--cache-dir PATH" "Path to cache directory for TRUD downloads"]
   [nil "--db FILENAME" "Name of database; usually optional. A default will be chosen if omitted"]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["Usage: clj -M:run <command> [options]"
        "  (or java -jar dmd.jar <command> [options])"
        "Commands:"
        "  - latest   : download and install the latest dm+d distribution"
        "  - list     : list available distributions from TRUD"
        "  - download : download and install a specific dm+d release (e.g. 2021-04-05)"
        "  - install  : install from a manually downloaded/unzipped distribution"
        "Options:"
        options-summary]
       (str/join \newline)))

(defn exit
  [status msg]
  (println msg)
  (System/exit status))

(defn download [{:keys [api-key cache-dir db]} [release-date]]
  (cond
    (not api-key)
    (exit 1 "Missing api-key. You must provide a path to a file containing TRUD api key")
    (not cache-dir)
    (exit 1 "Missing cache directory.")
    :else
    (install-release api-key cache-dir db release-date)))

(defn list-available [{:keys [api-key]}]
  (cond
    (not api-key)
    (exit 1 "Missing api-key. You must provide a path to a file containing TRUD api key")
    :else
    (dl/print-available-releases (str/trim-newline (slurp api-key)))))

(defn install [{:keys [db]} params]
  (cond
    (not= (count params) 1)
    (exit 1 "You must specify a directory from which to import.")
    (not db)
    (exit 1 "You must specify the name of the database for manual install.")
    :else
    (import-dmd db (first params))))

(defn -main [& args]
  (let [{:keys [options arguments summary errors]} (cli/parse-opts args cli-options)]
    (cond
      ;; asking for help?
      (:help options)
      (println (usage summary))
      ;; if we have any errors, exit with error message(s)
      errors
      (exit 1 (str/join \newline errors))
      ;; if we have no command, exit with error message
      (= 0 (count arguments))
      (exit 1 (str "invalid command\n" (usage summary)))
      ;; invoke command
      :else
      (let [command (str/lower-case (first arguments))
            params (next arguments)]                        ;; for future use
        (cond
          (= "latest" command) (download options nil)
          (= "list" command) (list-available options)
          (= "download" command) (download options params)
          (= "install" command) (install options params))))))

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

