(ns com.eldrix.dmd.live-test
  (:require [clojure.java.io :as io]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :as t :refer [deftest is use-fixtures]]
            [com.eldrix.dmd.core :as dmd]))

(stest/instrument)

(def ^:dynamic *db* nil)

(defn trud-api-key-filename
  "Returns the filename of the file containing the TRUD API key."
  []
  (or (System/getenv "TRUD_API_KEY_FILE") "api-key.txt"))

(defn live-test-fixture [f]
  (if (.exists (io/file "latest-dmd.db"))
    (println "WARNING: skipping test of install-release as using existing latest-dmd.db. Delete this file if required.")
    (dmd/install-release (trud-api-key-filename) "cache" "latest-dmd.db"))
  (binding [*db* (dmd/open-store "latest-dmd.db")]
    (f)
    (dmd/close *db*)))

(use-fixtures :once live-test-fixture)

(deftest ^:live simple-fetch
  (let [amlodipine-vtm (dmd/fetch-product-by-exact-name *db* "Amlodipine")
        amlodipine-vmps (dmd/vmps-for-product *db* (:VTM/VTMID amlodipine-vtm))]
    (is amlodipine-vtm)
    (is (:VTM/VTMID amlodipine-vtm))
    (is (seq amlodipine-vmps))
    (is (= amlodipine-vtm (:VMP/VTM (first amlodipine-vmps))))))

(comment
  (dmd/install-release "../trud/api-key.txt" "cache" "latest-dmd.db")
  (System/getenv "TRUD_API_KEY_FILE")
  (def *db* (dmd/open-store "latest-dmd.db"))
  (t/run-tests))
