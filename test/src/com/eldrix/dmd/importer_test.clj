(ns com.eldrix.dmd.importer-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer :all]
            [com.eldrix.dmd.import :as dim]
            [clojure.java.io :as io]))

(def dir "dmd-2021-08-26")

(deftest file-ordering
  (is (= '(:LOOKUP :INGREDIENT :VTM :VMP :AMP :VMPP :AMPP :GTIN :BNF) (map :type (dim/dmd-file-seq (io/resource dir))))))

(deftest import-lookups
  (is (= {:TYPE [:LOOKUP :BASIS_OF_NAME]
          :CD   1
          :DESC "rINN - Recommended International Non-proprietary"}
         (first (dim/get-component (io/resource dir) :LOOKUP :BASIS_OF_NAME)))))

(deftest metadata
  (is (= (java.time.LocalDate/of 2021 8 26) (:release-date (dim/get-release-metadata (io/resource dir))))))


(comment
  (dim/statistics-dmd (io/resource dir))
  (run-tests))