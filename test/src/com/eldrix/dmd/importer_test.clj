(ns com.eldrix.dmd.importer-test
  (:require [clojure.test :refer :all]
            [com.eldrix.dmd.import :as dim]
            [clojure.java.io :as io]))

(deftest file-ordering
  (is (= '(:LOOKUP :INGREDIENT :VTM :VMP :AMP :VMPP :AMPP) (map :type (dim/dmd-file-seq (io/resource "dmd-empty"))))))

(deftest import-lookups
  (is (= {:TYPE [:LOOKUP :BASIS_OF_NAME],
          :CD "0001",
          :DESC "rINN - Recommended International Non-proprietary",
          :ID :BASIS_OF_NAME-0001}
         (first (dim/get-component (io/resource "dmd-empty") :LOOKUP :BASIS_OF_NAME)))))

(deftest metadata
  (is (= (java.time.LocalDate/of 2021 8 26) (:release-date (dim/get-release-metadata (io/resource "dmd-empty"))))))


(comment
  (run-tests))