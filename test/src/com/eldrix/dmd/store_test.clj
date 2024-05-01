(ns com.eldrix.dmd.store-test
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.test :refer [deftest is run-tests]]
            [com.eldrix.dmd.core :as dmd]
            [clojure.java.io :as io])
  (:import (java.io File)
           (java.time LocalDate)))

(def dir "dmd-2021-08-26")

(stest/instrument)

(defn create-and-open-store []
  (let [filename (File/createTempFile "dmd-test" ".db")]
    (.delete filename)
    (dmd/install-from-dirs filename [(io/resource dir)] :batch-size 1)
    (dmd/open-store filename)))

;; test basic import, store and fetch across products.
(deftest import-store-and-fetch
  (let [st (create-and-open-store)
        co-amilofruse-vtm (dmd/fetch-product st 34186711000001102)
        co-amilofruse-vmp-1 (dmd/fetch-product st 318135008)
        co-amilofruse-vmp-2 (dmd/fetch-product st 318136009)
        bases-of-name (let [bases (dmd/fetch-lookup st :BASIS_OF_NAME)]
                        (zipmap (map :BASIS_OF_NAME/CD bases) (map :BASIS_OF_NAME/DESC bases)))]
    (is (= (LocalDate/of 2021 8 26) (dmd/fetch-release-date st)))
    (is (= "Co-amilofruse" (:VTM/NM co-amilofruse-vtm)))
    (is (not (:VMP/SUG_F co-amilofruse-vmp-1)))
    (is (:VMP/SUG_F co-amilofruse-vmp-2))
    (is (= (:VMP/VTM co-amilofruse-vmp-1) (:VMP/VTM co-amilofruse-vmp-2) co-amilofruse-vtm))
    (is (= "tablet" (get-in co-amilofruse-vmp-2 [:VMP/UDFS_UOM :UNIT_OF_MEASURE/DESC])))
    (is (= "Discrete" (get-in co-amilofruse-vmp-2 [:VMP/DF_IND :DF_INDICATOR/DESC])))
    (is (= "Tablet" (get-in co-amilofruse-vmp-2 [:VMP/DRUG_FORM :FORM/DESC])))
    (is (= (:VMP/BASISCD co-amilofruse-vmp-2) (get-in co-amilofruse-vmp-2 [:VMP/BASIS :BASIS_OF_NAME/CD])))
    (is (= (get-in co-amilofruse-vmp-2 [:VMP/BASIS :BASIS_OF_NAME/DESC]) (get bases-of-name 2)))
    (is (= #{387516008 387475002} (set (map :VMP__VIRTUAL_PRODUCT_INGREDIENT/ISID (:VMP/VIRTUAL_PRODUCT_INGREDIENTS co-amilofruse-vmp-2)))))
    (is (= 2 (count (:VMP/VIRTUAL_PRODUCT_INGREDIENTS co-amilofruse-vmp-2))))
    ;; are we getting linked relationships right?
    (is (= 1
           (:VMP/PRES_STATCD co-amilofruse-vmp-2)
           (get-in co-amilofruse-vmp-2 [:VMP/PRES_STAT :VIRTUAL_PRODUCT_PRES_STATUS/CD])))
    ;; fetch the AMP and is the linked VMP the same as we know?
    (is (= (:AMP/VP (dmd/fetch-product st 37365811000001102)) co-amilofruse-vmp-2))
    (is (= "C03EB01"
           (get-in co-amilofruse-vmp-2 [:VMP/BNF_DETAILS :BNF_DETAILS/ATC])
           (dmd/atc-for-product st 318136009)))
    (is (= "C03EB01" (dmd/atc-for-product st 34186711000001102))) ;; test from VTM
    (is (= "C03EB01" (dmd/atc-for-product st 37365811000001102))) ;; test from AMP
    (is (= "C03EB01" (dmd/atc-for-product st 37365911000001107))) ;; test from AMPP
    (is (empty? (dmd/vpids-from-atc st "C03")) "Must use an explicit wildcard for ATC matching; no longer does prefix search without asking")
    (is (= #{318136009}
           (set (dmd/vpids-from-atc st "C03*"))
           (set (dmd/vpids-from-atc st "C03EB01"))
           (set (dmd/vpids-from-atc st "C0?EB01"))
           (set (dmd/vpids-from-atc st "C03EB0?"))))
    (is (= ["mg" "mg"] (map #(get-in % [:VMP__VIRTUAL_PRODUCT_INGREDIENT/STRNT_NMRTR_UOM :UNIT_OF_MEASURE/DESC]) (:VMP/VIRTUAL_PRODUCT_INGREDIENTS co-amilofruse-vmp-2))))
    (is (= #{318136009 318135008} (set (map :VMP/VPID (dmd/vmps-for-product st 34186711000001102)))))
    (is (= 34186711000001102 (:VTM/VTMID (first (dmd/vtms-for-product st 318135008)))))
    (.close st)))

(comment
  (run-tests)
  (def st (create-and-open-store))
  (dmd/fetch-product st 318135008)
  (dmd/vtms-for-product st 318135008)
  (dmd/vmps-for-product st 34186711000001102)
  (dmd/fetch-product st 34186711000001102)
  (dmd/amps-for-product st 34186711000001102)
  (dmd/vtms-for-product st 37365811000001102)
  (dmd/atc-for-product st 34186711000001102))

