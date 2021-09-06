(ns com.eldrix.dmd.store-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer :all]
            [com.eldrix.dmd.core :as dmd]
            [clojure.java.io :as io]
            [com.eldrix.dmd.import :as dim]
            [datalevin.core :as d]
            [com.eldrix.dmd.store2 :as st2])
  (:import [java.nio.file Files]
           (java.nio.file.attribute FileAttribute)
           (clojure.lang ExceptionInfo)))

(def dir "dmd-2021-08-26")

(defn create-and-open-store []
  (let [db-dir (.getAbsolutePath (.toFile (Files/createTempDirectory "dmd-test" (into-array FileAttribute []))))]
    (dmd/install-from-dirs db-dir [(io/resource dir)])
    (dmd/open-store db-dir)))

;; ensure that importing an unknown ingredient throws an exception
(deftest import-validation
  (let [st (create-and-open-store)
        ap-ing-exists {:TYPE [:AMP :AP_INGREDIENT], :APID 37365811000001102, :ISID 387516008}
        ap-ing-not-exists {:TYPE [:AMP :AP_INGREDIENT] :APID 37365811000001102 :ISID 123}]
    (is (d/transact! (.-conn st) [(st2/parse ap-ing-exists)]))
    (is (thrown? ExceptionInfo (d/transact! (.-conn st) [(st2/parse ap-ing-not-exists)])))))

;; test basic import, store and fetch across products.
(deftest import-store-and-fetch
  (let [st (create-and-open-store)
        co-amilofruse-vtm (dmd/fetch-product st 34186711000001102)
        co-amilofruse-vmp-1 (dmd/fetch-product st 318135008)
        co-amilofruse-vmp-2 (dmd/fetch-product st 318136009)
        bases-of-name (let [bases (dmd/fetch-lookup st :BASIS_OF_NAME)]
                        (zipmap (map :BASIS_OF_NAME/CD bases) (map :BASIS_OF_NAME/DESC bases)))]
    (is (= "Co-amilofruse" (:VTM/NM co-amilofruse-vtm)))
    (is (not (:VMP/SUG_F co-amilofruse-vmp-1)))
    (is (:VMP/SUG_F co-amilofruse-vmp-2))
    (is (= (:VMP/VTM co-amilofruse-vmp-1) (:VMP/VTM co-amilofruse-vmp-2) co-amilofruse-vtm))
    (is (= "tablet" (get-in co-amilofruse-vmp-2 [:VMP/UDFS_UOM :UNIT_OF_MEASURE/DESC])))
    (is (= "Discrete" (get-in co-amilofruse-vmp-2 [:VMP/DF_IND :DF_INDICATOR/DESC])))
    (is (= "Tablet" (get-in co-amilofruse-vmp-2 [:VMP/DRUG_FORM :FORM/DESC])))
    (is (= (:VMP/BASISCD co-amilofruse-vmp-2) (get-in co-amilofruse-vmp-2 [:VMP/BASIS :BASIS_OF_NAME/CD])))
    (is (= (get-in co-amilofruse-vmp-2 [:VMP/BASIS :BASIS_OF_NAME/DESC]) (get bases-of-name "0002")))
    (is (= #{387516008 387475002} (set (map :VPI/ISID (:VMP/INGREDIENTS co-amilofruse-vmp-2)))))
    (is (= 2 (count (:VMP/INGREDIENTS co-amilofruse-vmp-2))))
    ;; are we getting linked relationships right?
    (is (= "0001"
           (:VMP/PRES_STATCD co-amilofruse-vmp-2)
           (get-in co-amilofruse-vmp-2 [:VMP/PRES_STAT :VIRTUAL_PRODUCT_PRES_STATUS/CD])))
    ;; fetch the AMP and is the linked VMP the same as we know?
    (is (= (:AMP/VP (dmd/fetch-product st 37365811000001102)) co-amilofruse-vmp-2))
    (is (= "C03EB01" (get-in co-amilofruse-vmp-2 [:VMP/BNF_DETAILS :BNF_DETAILS/ATC])))
    (is (= "mg" (get-in co-amilofruse-vmp-2 [:VMP/BNF_DETAILS :BNF_DETAILS/DDD_UOM :UNIT_OF_MEASURE/DESC])))
    (.close st)))

(comment
  (run-tests)
  (import-validation)
  (def st (create-and-open-store))
  (dmd/fetch-product st 318136009)
  (d/q '[:find (pull ?e [*])
         :where
         [?e :PRODUCT/TYPE :AMPP]] (d/db (.-conn st)))

  (st2/parse (first (dim/get-component (io/resource dir) :BNF :VMPS)))

  )