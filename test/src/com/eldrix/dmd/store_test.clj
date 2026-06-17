(ns com.eldrix.dmd.store-test
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.test :refer [deftest is run-tests]]
            [com.eldrix.dmd.core :as dmd]
            [com.eldrix.dmd.store :as st]
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
      (jdbc/execute-one! conn [(str "PRAGMA application_id = " st/application-id)])
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

(deftest history-and-queries
  (let [st (create-and-open-store)]
    (is (= #{354303007} (dmd/previous-ids st 34186711000001102)))
    (is (= #{34186711000001102} (dmd/current-ids st 354303007)))
    (is (= #{} (dmd/current-ids st 34186711000001102)) "self entries must be excluded")
    (is (= #{} (dmd/previous-ids st 2070501000001104)) "supplier has only a self entry")
    (let [history (dmd/fetch-history st 318135008)]
      (is (= 2 (count history)))
      (is (= ["10406411000001101" "318135008"] (map (comp str :HISTORY/IDPREVIOUS) history)) "ordered by start date")
      (is (= "VMP" (:HISTORY/CLS (first history)))))
    (is (= #{387516008 387475002} (set (dmd/isids-for-vtm st 34186711000001102))))
    (is (= [34186711000001102] (dmd/vtmids-for-ingredient st 387475002)))
    (is (= #{"Co-amilofruse 2.5mg/20mg tablets" "Co-amilofruse 5mg/40mg tablets"}
           (into #{} (map :NM) (dmd/plan-products st :VMP))))
    (is (= 3 (count (into [] (map :APID) (dmd/plan-products st :AMP)))))
    (is (contains? dmd/lookup-types :BASIS_OF_NAME))
    (is (= 26 (count dmd/lookup-types)))
    (is (every? #(seq (dmd/fetch-lookup st %)) dmd/lookup-types) "every lookup type enumerable and populated")
    (dmd/close st)))

(deftest traversal-and-subsumption
  (let [st (create-and-open-store)
        vtm 34186711000001102
        vmp-1 318135008                                     ;; co-amilofruse 2.5mg/20mg
        vmp-2 318136009                                     ;; co-amilofruse 5mg/40mg
        amp 37365811000001102                               ;; Mawdsley-Brooks; an AMP of vmp-2
        other-amp 38847311000001102                         ;; Medihealth; another AMP of vmp-2
        vmpp 1245011000001108                               ;; 28 tablet pack of vmp-2
        comb-vmpp 8967511000001109                          ;; combination pack
        ampp 37365911000001107                              ;; pack of amp, within vmpp
        comb-ampp 8968011000001101]                         ;; combination pack of amp
    (is (= :VTM (dmd/product-type st vtm)))
    (is (= :VMP (dmd/product-type st vmp-2)))
    (is (= :AMP (dmd/product-type st amp)))
    (is (= :VMPP (dmd/product-type st vmpp)))
    (is (= :AMPP (dmd/product-type st ampp)))
    (is (nil? (dmd/product-type st 999)))
    ;; identifier-level traversal
    (is (= #{vmp-1 vmp-2} (set (dmd/vpids-for-product st vtm))))
    (is (= [vmp-2] (dmd/vpids-for-product st ampp)))
    (is (= #{vtm} (set (dmd/vtmids-for-product st amp))))
    (is (= #{vtm} (set (dmd/vtmids-for-product st ampp))) "VTM must be reachable from an AMPP")
    (is (= #{amp other-amp 37706811000001108} (set (dmd/apids-for-product st vmpp)))
        "AMPs must be reachable from a VMPP, via the VMP")
    (is (= #{vmpp comb-vmpp} (set (dmd/vppids-for-product st vtm))))
    (is (= #{ampp comb-ampp} (set (dmd/appids-for-product st vtm))))
    (is (= [ampp] (dmd/appids-for-product st vmpp)))
    ;; reverse combination-pack traversal: a child pack resolves to its parent pack
    (is (= [comb-vmpp] (dmd/parent-vppids-for-vppid st vmpp)))
    (is (= [comb-ampp] (dmd/parent-appids-for-appid st ampp)))
    (is (empty? (dmd/parent-vppids-for-vppid st comb-vmpp)) "a parent combination pack is not itself a child")
    (is (empty? (dmd/parent-appids-for-appid st comb-ampp)))
    (is (= #{vtm} (dmd/vtmids-for-product st vtm)) "a VTM's VTM is itself, consistently a set")
    (is (= vtm (:VTM/VTMID (first (dmd/vtms-for-product st ampp)))))
    (is (= #{vmpp comb-vmpp} (into #{} (map :VMPP/VPPID) (dmd/vmpps-for-product st vtm))))
    (is (= [ampp] (mapv :AMPP/APPID (dmd/ampps-for-product st vmpp))))
    ;; subsumption: a VTM subsumes everything beneath it...
    (is (dmd/subsumes? st vtm vmp-2))
    (is (dmd/subsumes? st vtm amp))
    (is (dmd/subsumes? st vtm vmpp))
    (is (dmd/subsumes? st vtm ampp))
    ;; ...a VMP subsumes its AMPs, VMPPs and AMPPs...
    (is (dmd/subsumes? st vmp-2 amp))
    (is (dmd/subsumes? st vmp-2 vmpp))
    (is (dmd/subsumes? st vmp-2 ampp))
    (is (not (dmd/subsumes? st vmp-1 amp)) "a sibling VMP subsumes nothing here")
    (is (not (dmd/subsumes? st vmp-1 ampp)))
    ;; ...an AMPP is both its AMP and its VMPP, but not other packs...
    (is (dmd/subsumes? st amp ampp))
    (is (dmd/subsumes? st amp comb-ampp))
    (is (not (dmd/subsumes? st other-amp ampp)))
    (is (dmd/subsumes? st vmpp ampp))
    (is (dmd/subsumes? st comb-vmpp comb-ampp))
    (is (not (dmd/subsumes? st comb-vmpp ampp)))
    ;; ...and subsumption is strict, directional, and false for unknowns
    (is (not (dmd/subsumes? st vtm vtm)) "a product does not subsume itself")
    (is (not (dmd/subsumes? st vmp-2 vtm)))
    (is (not (dmd/subsumes? st ampp vmpp)))
    (is (not (dmd/subsumes? st 999 vmp-2)))
    (is (not (dmd/subsumes? st vtm 999)))
    ;; ingredient enumeration and single-code lookups
    (is (= 4 (count (into [] (map :ISID) (dmd/plan-ingredients st)))))
    (is (contains? (into #{} (map :NM) (dmd/plan-ingredients st)) "Furosemide"))
    (is (= "POM" (:LEGAL_CATEGORY/DESC (dmd/fetch-lookup st :LEGAL_CATEGORY 3))))
    (is (nil? (dmd/fetch-lookup st :LEGAL_CATEGORY 999)))
    (dmd/close st)))

(deftest search-products
  (let [st (create-and-open-store)]
    (is (= #{"VTM" "VMP" "AMP" "VMPP" "AMPP"}
           (into #{} (map :SEARCH/TYPE) (dmd/search st "co-amilofruse"))))
    (is (= #{318135008 318136009}
           (into #{} (map :SEARCH/ID) (dmd/search st "co-amilof" :types #{:VMP}))) "prefix search")
    (is (= #{318136009}
           (into #{} (map :SEARCH/ID) (dmd/search st "amilofruse 40mg" :types #{:VMP}))) "multiple tokens combine as AND")
    (is (= 1 (count (dmd/search st "co-amilofruse" :limit 1))))
    (is (= [] (dmd/search st "")))
    (is (= [] (dmd/search st "   ")))
    (is (= [] (dmd/search st nil)))
    (is (= [] (dmd/search st "\" OR 1=1")) "FTS5 query syntax must not be interpreted")
    (dmd/close st)))

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
    (is (= {:VTM 1 :VMP 2 :AMP 3 :VMPP 2 :AMPP 2 :INGREDIENT 4 :HISTORY 10 :VTM_INGREDIENT 2 :GTIN 2 :BNF 1}
           counts))
    (dmd/close st)))

(deftest gtins
  (let [st (create-and-open-store)]
    ;; the fixture AMPP has a current GTIN (started 2019-03-13, no end date)
    ;; and an expired GTIN (2015-06-01 to 2019-03-12 inclusive)
    (is (= ["5037563003235"] (dmd/gtins-for-appid st 37365911000001107))
        "expired GTINs must be omitted by default")
    (is (= #{"5037563003235" "5012617019844"}
           (set (dmd/gtins-for-appid st 37365911000001107 :include-expired true))))
    (is (= [37365911000001107] (dmd/appids-from-gtin st "5037563003235")))
    (is (= [37365911000001107] (dmd/appids-from-gtin st 5037563003235))
        "a GTIN may be given as a number")
    (is (= [] (dmd/appids-from-gtin st "5012617019844"))
        "expired assignments must be omitted by default")
    (is (= [37365911000001107] (dmd/appids-from-gtin st "5012617019844" :include-expired true)))
    (is (= [37365911000001107] (dmd/appids-from-gtin st "5012617019844" :on-date (LocalDate/of 2017 1 1)))
        "valid during its period of validity")
    (is (= [37365911000001107] (dmd/appids-from-gtin st "5012617019844" :on-date (LocalDate/of 2019 3 12)))
        "valid on its end date inclusive")
    (is (= [] (dmd/appids-from-gtin st "5037563003235" :on-date (LocalDate/of 2019 3 12)))
        "not valid before its start date")
    (is (= ["5037563003235"] (dmd/gtins-for-appid st 37365911000001107 :on-date (LocalDate/of 2019 3 13)))
        "valid on its start date")
    (is (= [] (dmd/gtins-for-appid st 999)) "unknown AMPP")
    (is (= [] (dmd/appids-from-gtin st "0000000000000")) "unknown GTIN")
    (dmd/close st)))

(deftest extended-product-information
  (let [st (create-and-open-store)
        amp (dmd/fetch-product st 37365811000001102)
        vmpp (dmd/fetch-product st 1245011000001108)
        ampp (dmd/fetch-product st 37365911000001107)
        comb-vmpp (dmd/fetch-product st 8967511000001109)
        comb-ampp (dmd/fetch-product st 8968011000001101)]
    ;; AMP: excipients, licensed routes and additional product information
    (is (= #{"Propylene glycol" "Butylated hydroxyanisole"}
           (into #{} (map #(get-in % [:AMP__AP_INGREDIENT/IS :INGREDIENT/NM]))
                 (:AMP/AP_INGREDIENTS amp))))
    (is (= ["Oral"] (mapv #(get-in % [:AMP__LICENSED_ROUTE/ROUTE :ROUTE/DESC])
                          (:AMP/LICENSED_ROUTES amp))))
    (is (= "8.5mm" (get-in amp [:AMP/AP_INFORMATION :AMP__AP_INFORMATION/SZ_WEIGHT])))
    (is (= "White" (get-in amp [:AMP/AP_INFORMATION :AMP__AP_INFORMATION/COLOUR :COLOUR/DESC])))
    (is (empty? (:AMP/AP_INGREDIENTS (dmd/fetch-product st 38847311000001102)))
        "no excipients recorded for this AMP")
    ;; VMPP: drug tariff information
    (is (= "529" (get-in vmpp [:VMPP/DRUG_TARIFF_INFO :VMPP__DRUG_TARIFF_INFO/PRICE])))
    (is (= "Part VIIIA Category C"
           (get-in vmpp [:VMPP/DRUG_TARIFF_INFO :VMPP__DRUG_TARIFF_INFO/PAY_CAT :DT_PAYMENT_CATEGORY/DESC])))
    ;; AMPP: appliance pack, prescribing, price and reimbursement information
    (is (= "Allowed (in Drug Tariff)"
           (get-in ampp [:AMPP/APPLIANCE_PACK_INFO :AMPP__APPLIANCE_PACK_INFO/REIMB_STAT :REIMBURSEMENT_STATUS/DESC])))
    (is (get-in ampp [:AMPP/DRUG_PRODUCT_PRESCRIB_INFO :AMPP__DRUG_PRODUCT_PRESCRIB_INFO/ACBS]))
    (is (get-in ampp [:AMPP/DRUG_PRODUCT_PRESCRIB_INFO :AMPP__DRUG_PRODUCT_PRESCRIB_INFO/HOSP]))
    (is (= 3384 (get-in ampp [:AMPP/MEDICINAL_PRODUCT_PRICE :AMPP__MEDICINAL_PRODUCT_PRICE/PRICE])))
    (is (= "NHS Indicative Price"
           (get-in ampp [:AMPP/MEDICINAL_PRODUCT_PRICE :AMPP__MEDICINAL_PRODUCT_PRICE/PRICE_BASIS :PRICE_BASIS/DESC])))
    (is (get-in ampp [:AMPP/REIMBURSEMENT_INFO :AMPP__REIMBURSEMENT_INFO/PX_CHRGS]))
    (is (= "Special container"
           (get-in ampp [:AMPP/REIMBURSEMENT_INFO :AMPP__REIMBURSEMENT_INFO/SPEC_CONT :SPEC_CONT/DESC])))
    (is (= "Discount not deducted - automatic"
           (get-in ampp [:AMPP/REIMBURSEMENT_INFO :AMPP__REIMBURSEMENT_INFO/DND_IND :DND/DESC])))
    ;; combination packs and their content
    (is (= "Combination pack" (get-in comb-vmpp [:VMPP/COMBPACK :COMBINATION_PACK_IND/DESC])))
    (is (= [1245011000001108]
           (mapv #(get-in % [:VMPP__COMB_CONTENT/CHLD :VMPP/VPPID]) (:VMPP/COMB_CONTENT comb-vmpp))))
    (is (= [37365911000001107]
           (mapv #(get-in % [:AMPP__COMB_CONTENT/CHLD :AMPP/APPID]) (:AMPP/COMB_CONTENT comb-ampp))))
    (is (empty? (:VMPP/COMB_CONTENT vmpp)) "not a combination pack")
    (is (empty? (:AMPP/COMB_CONTENT ampp)) "not a combination pack")
    (is (= [8967511000001109] (mapv :VMPP/VPPID (dmd/parent-packs-for-vppid st 1245011000001108)))
        "parent-packs-for-vppid should resolve a child VMPP to its parent combination pack VMPP")
    (is (= [8968011000001101] (mapv :AMPP/APPID (dmd/parent-packs-for-appid st 37365911000001107)))
        "parent-packs-for-appid should resolve a child AMPP to its parent combination pack AMPP")
    ;; ingredient by identifier
    (is (= "Furosemide" (:INGREDIENT/NM (dmd/fetch-ingredient st 387475002))))
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
             (set (map #(get-in % [:VMP__VIRTUAL_PRODUCT_INGREDIENT/IS :INGREDIENT/ISIDPREV])
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

