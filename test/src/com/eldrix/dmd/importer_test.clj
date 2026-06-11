(ns com.eldrix.dmd.importer-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer :all]
            [com.eldrix.dmd.import :as dim]
            [clojure.java.io :as io]))

(def dir "dmd-2021-08-26")

(deftest file-ordering
  (is (= '(:LOOKUP :INGREDIENT :VTM :VMP :AMP :VMPP :AMPP :GTIN :BNF :HISTORY :VTM_ING)
         (map :type (dim/dmd-file-seq (io/resource dir))))))

(deftest import-lookups
  (is (= {:TYPE [:LOOKUP :BASIS_OF_NAME]
          :CD   1
          :DESC "rINN - Recommended International Non-proprietary"}
         (first (dim/get-component (io/resource dir) :LOOKUP :BASIS_OF_NAME)))))

(deftest import-history
  (let [history (dim/get-component (io/resource dir) :HISTORY :HISTORY)
        by-cls (group-by :CLS history)]
    (is (= #{"VTM" "VMP" "ING" "SUPP" "FORM" "ROUTE" "UOM"} (set (keys by-cls))))
    (is (= {:TYPE       [:HISTORY :HISTORY]
            :CLS        "VTM"
            :IDCURRENT  34186711000001102
            :IDPREVIOUS 354303007
            :STARTDT    (java.time.LocalDate/of 2004 12 1)
            :ENDDT      (java.time.LocalDate/of 2017 3 9)}
           (first (get by-cls "VTM"))))
    (let [self-row (second (get by-cls "VTM"))]    ;; rows with IDCURRENT=IDPREVIOUS record current id validity
      (is (= (:IDCURRENT self-row) (:IDPREVIOUS self-row)))
      (is (nil? (:ENDDT self-row))))))

(deftest import-vtm-ingredients
  (is (= [{:TYPE [:VTM_ING :VTM_ING] :VTMID 34186711000001102 :ISID 387516008}
          {:TYPE [:VTM_ING :VTM_ING] :VTMID 34186711000001102 :ISID 387475002}]
         (dim/get-component (io/resource dir) :VTM_ING :VTM_ING))))

(deftest metadata
  (is (= (java.time.LocalDate/of 2021 8 26) (:release-date (dim/get-release-metadata (io/resource dir))))))


(comment
  (dim/statistics-dmd (io/resource dir))
  (run-tests))