(ns com.eldrix.dmd.cli
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [com.eldrix.dmd.core :as dmd]
            [com.eldrix.dmd.serve :as serve]
            [clojure.tools.logging.readable :as log]))

(def cli-options
  [[nil "--api-key PATH" "Path to file containing TRUD API key"]
   [nil "--cache-dir PATH" "Path to cache directory for TRUD downloads"]
   [nil "--db FILENAME" "Name of database; usually optional. A default will be chosen if omitted"]
   ["-p" "--port PORT" "Port to use"]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["Usage: clj -M:run <command> [options]"
        "  (or java -jar dmd.jar <command> [options])"
        "Commands:"
        "  - latest   : download and install the latest dm+d distribution"
        "  - list     : list available distributions from TRUD"
        "  - download : download and install a specific dm+d release (e.g. 2021-04-05)"
        "  - install  : install from a manually downloaded/unzipped distribution"
        "  - serve    : run a HTTP server"
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
    (dmd/install-release api-key cache-dir db release-date)))

(defn list-available [{:keys [api-key]}]
  (cond
    (not api-key)
    (exit 1 "Missing api-key. You must provide a path to a file containing TRUD api key")
    :else
    (dmd/print-available-releases (str/trim-newline (slurp api-key)))))

(defn install [{:keys [db]} params]
  (cond
    (not= (count params) 1)
    (exit 1 "You must specify a directory from which to import.")
    (not db)
    (exit 1 "You must specify the name of the database for manual install.")
    :else
    (dmd/install-from-dirs db params)))

(defn run-server [{:keys [db port] :or {port "8080"}}]
  (if db
    (let [st (dmd/open-store db)]
      (log/info "starting server using " db " on port" port)
      (try (serve/start-server st (Integer/parseInt port))
           (catch NumberFormatException e
             (log/error "invalid port:" port))))
    (log/error "You must specify a database using --db")))

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
          (= "install" command) (install options params)
          (= "serve" command) (run-server options))))))

(comment
  )

