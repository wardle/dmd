(ns com.eldrix.dmd.store-test
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.test :refer [deftest is run-tests]]
            [com.eldrix.dmd.core :as dmd]
            [com.eldrix.dmd.store4 :as st4]
            [clojure.java.io :as io]
            [next.jdbc :as jdbc])
  (:import (java.io File)
           (java.time LocalDate)))

(def dir "dmd-2021-08-26")

(stest/instrument)

(defn create-and-open-store []
  (let [filename (File/createTempFile "dmd-test" ".db")]
    (.delete filename)
    (dmd/install-from-dirs filename [(io/resource dir)] :batch-size 1)
    (dmd/open-store filename)))

(deftest open-invalid-files
  (let [not-sqlite (doto (File/createTempFile "dmd-test" ".db") (spit "this is not a database"))
        not-dmd (File/createTempFile "dmd-test" ".db")
        wrong-version (File/createTempFile "dmd-test" ".db")]
    (.delete not-dmd)
    (.delete wrong-version)
    (with-open [conn (jdbc/get-connection (str "jdbc:sqlite:" not-dmd))]
      (jdbc/execute-one! conn ["create table TEST (id integer)"]))
    (with-open [conn (jdbc/get-connection (str "jdbc:sqlite:" wrong-version))]
      (jdbc/execute-one! conn [(str "PRAGMA application_id = " st4/application-id)])
      (jdbc/execute-one! conn ["PRAGMA user_version = 1"])
      (jdbc/execute-one! conn ["create table TEST (id integer)"]))
    (is (thrown-with-msg? Exception #"file not found" (dmd/open-store "no-such-file.db")))
    (is (thrown-with-msg? Exception #"not a SQLite database" (dmd/open-store not-sqlite)))
    (is (not (dmd/sqlite-database? not-sqlite)))
    (is (thrown-with-msg? Exception #"not a dm\+d database" (dmd/open-store not-dmd)))
    (is (dmd/sqlite-database? not-dmd))
    (is (not (dmd/dmd-database? not-dmd)))
    (is (thrown-with-msg? Exception #"incompatible dm\+d database version" (dmd/open-store wrong-version)))
    (is (dmd/dmd-database? wrong-version))
    (run! #(.delete ^File %) [not-sqlite not-dmd wrong-version])))

(deftest store-status
  (let [st (create-and-open-store)
        {:keys [version created release trud files counts]} (dmd/status st)]
    (is (= 2 version))
    (is created)
    (is (= (LocalDate/of 2021 8 26) release))
    (is (nil? trud) "no TRUD provenance when installed from local directories")
    (is (= 11 (count files)))
    (is (= #{:LOOKUP :INGREDIENT :VTM :VMP :AMP :VMPP :AMPP :GTIN :BNF :HISTORY :VTM_ING}
           (set (map :type files))))
    (is (= {:VTM 1 :VMP 2 :AMP 3 :VMPP 1 :AMPP 1 :INGREDIENT 4 :HISTORY 10 :VTM_INGREDIENT 2 :GTIN 1 :BNF 1}
           counts))
    (dmd/close st)))

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
    ;; fields previously dropped on import (schema v2)
    (is (= "Co-amilofruse 2.5/20 tablets" (:VMP/NMPREV co-amilofruse-vmp-1)))
    (is (= "2004-05-04" (:VMP/NMDT co-amilofruse-vmp-1)))
    (is (:AMP/PARALLEL_IMPORT (dmd/fetch-product st 37706811000001108)))
    (is (not (:AMP/INVALID (dmd/fetch-product st 37706811000001108))))
    (let [suppliers (zipmap (map :SUPPLIER/CD (dmd/fetch-lookup st :SUPPLIER))
                            (dmd/fetch-lookup st :SUPPLIER))]
      (is (:SUPPLIER/INVALID (get suppliers 3145201000001108)))
      (is (= 2073601000001105 (:SUPPLIER/CDPREV (get suppliers 15883511000001102))))
      (is (= "2009-08-07" (:SUPPLIER/CDDT (get suppliers 15883511000001102)))))
    (let [forms (zipmap (map :FORM/CD (dmd/fetch-lookup st :FORM)) (dmd/fetch-lookup st :FORM))]
      (is (= 385098002 (:FORM/CDPREV (get forms 35366811000001106)))))
    (let [ingredients (dmd/fetch-product st 318136009)]   ;; ingredient previous ids now stored
      (is (= #{3512011000001109 3536911000001101}
             (set (map #(get-in % [:VMP__VIRTUAL_PRODUCT_INGREDIENT/IS 0 :INGREDIENT/ISIDPREV])
                       (:VMP/VIRTUAL_PRODUCT_INGREDIENTS ingredients))))))
    (dmd/close st)))

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

