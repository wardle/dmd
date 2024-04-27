(ns com.eldrix.dmd.live-test
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :as t :refer [deftest testing is use-fixtures]]
            [com.eldrix.dmd.core :as dmd]
            [clojure.test :as t]))

(stest/instrument)

(def ^:dynamic *db* nil)

(defn trud-api-key-filename
  "Returns the filename of the file containing the TRUD API key."
  []
  (or (System/getenv "TRUD_API_KEY_FILE") "api-key.txt"))

(defn live-test-fixture [f]
  (dmd/install-release (trud-api-key-filename) "cache" "latest-dmd.db")
  (binding [*db* (dmd/open-store "latest-dmd.db")]
    (f)
    (dmd/close *db*)))

(use-fixtures :once live-test-fixture)

(deftest simple-fetch
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
