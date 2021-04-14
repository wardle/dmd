(ns com.eldrix.dmd.download
  (:require [clojure.pprint :as pprint]
            [clojure.string :as str]
            [com.eldrix.trud.core :as trud]
            [com.eldrix.trud.zip :as zipfile]
            [com.eldrix.dmd.import :as dim])
  (:import (java.time.format DateTimeFormatter)
           (java.time LocalDate)))


(def dm+d-trud-identifier 24)


(defn fetch-available-releases
  "Return a list of the available releases."
  [api-key]
  (trud/get-releases api-key dm+d-trud-identifier))

(defn print-available-releases
  [api-key]
  (pprint/print-table [:id :releaseDate :name] (fetch-available-releases api-key)))

(defn download-release
  "Download a dm+d distribution from TRUD
  Parameters:
  - api-key      : api key
  - cache-dir    : TRUD download cache directory (e.g. \"/var/trud/cache/\".
  - release-date : optional, release date of distribution (default, latest).
                 : can be a string or a java.time.LocalDate for convenience.

  Returns TRUD metadata about the item specified, but also the path to the
  downloaded archive file.
  See com.eldrix.trud.core/get-latest for information about return value."
  ([api-key cache-dir] (download-release api-key cache-dir nil))
  ([api-key cache-dir release-date]
   (if-let [release-date' (cond (instance? LocalDate release-date) release-date
                                (string? release-date) (LocalDate/parse release-date DateTimeFormatter/ISO_DATE))]
     ;; if we have an explicit release date, find it in the releases available, and then download it.
     (let [available (fetch-available-releases api-key)]
       (if-let [release (first (filter #(= release-date' (:releaseDate %)) available))] ;; find the release based on the release date specified
         (assoc release :archiveFilePath (trud/download-release cache-dir release)) ;; and once found, download it.
         (throw (ex-info "No release found for date" {:release-date release-date' :available (map :releaseDate available)}))))
     ;; if we don't have an explicit release date, get the latest
     (trud/get-latest {:api-key   api-key
                       :cache-dir cache-dir}
                      dm+d-trud-identifier))))

(comment
  (def api-key (str/trim-newline (slurp "/Users/mark/Dev/trud/api-key.txt")))
  api-key

  (print-available-releases api-key)
  (def release (download-release api-key "/var/tmp/trud"))

  )