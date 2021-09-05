(ns com.eldrix.dmd.store-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer :all]
            [com.eldrix.dmd.core :as dmd]
            [clojure.java.io :as io])
  (:import [java.nio.file Files]
           (java.nio.file.attribute FileAttribute)))

(def dir "dmd-2021-08-26")

(deftest create-store
  (let [db-dir (.getAbsolutePath (.toFile (Files/createTempDirectory "dmd-test" (into-array FileAttribute []))))
        _ (dmd/install-from-dirs db-dir [(io/resource dir)])
        st (dmd/open-store db-dir)]
    (is (= "Co-amilofruse" (:VTM/NM (dmd/fetch-product st 34186711000001102))))
    (is (not (:VMP/SUG_F (dmd/fetch-product st 318135008))))
    (is  (:VMP/SUG_F (dmd/fetch-product st 318136009)))
    (is (= (:VMP/VTM (dmd/fetch-product st 318135008)) (dmd/fetch-product st 34186711000001102)))))

(comment
  (run-tests))