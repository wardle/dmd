(ns com.eldrix.dmd.importer-test
  (:require [clojure.test :refer :all]
            [com.eldrix.dmd.import :as dim]
            [clojure.java.io :as io]))

(deftest file-ordering
  (is (= '(:LOOKUP :INGREDIENT :VTM :VMP :AMP :VMPP :AMPP) (map :type (dim/dmd-file-seq (io/resource "dmd-empty"))))))

(comment
  (run-tests))