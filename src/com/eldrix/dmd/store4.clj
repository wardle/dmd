(ns com.eldrix.dmd.store4
  "dm+d storage using sqlite"
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.eldrix.dmd.import :as dim]
            [honey.sql :as sql]
            [next.jdbc :as jdbc])
  (:import (java.time LocalDate LocalDateTime)))

(set! *warn-on-reflection* true)

(def store-version 1)

(s/def ::conn any?)
(s/def ::id (s/or :kw keyword? :coll (s/coll-of keyword?)))
(s/def ::create string?)
(s/def ::insert string?)
(s/def ::data fn?)
(s/def ::entity (s/keys :req-un [::id ::create] :opt-un [::insert ::data]))

(s/def ::lookup (s/or ::str string? ::sym symbol? ::kw keyword?))
(s/def ::code (s/nilable int?))
(s/def ::vtmid int?)
(s/def ::isid int?)
(s/def ::vpid int?)
(s/def ::vppid int?)
(s/def ::apid int?)
(s/def ::appid int?)
(s/def ::product-id int?)
(s/def ::atc string?)

(def entities
  [{:id     :metadata
    :create "create table METADATA (version integer, created text, release text)"}
   {:id     [:LOOKUP :COMBINATION_PACK_IND]
    :create "create table COMBINATION_PACK_IND (CD integer primary key, DESC text)"
    :insert "insert into COMBINATION_PACK_IND(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :COMBINATION_PROD_IND]
    :create "create table COMBINATION_PROD_IND (CD integer primary key, DESC text)"
    :insert "insert into COMBINATION_PROD_IND(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :BASIS_OF_NAME]
    :create "create table BASIS_OF_NAME (CD integer primary key, DESC text)"
    :insert "insert into BASIS_OF_NAME(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :NAMECHANGE_REASON]
    :create "create table NAMECHANGE_REASON (CD integer primary key, DESC text)"
    :insert "insert into NAMECHANGE_REASON(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :VIRTUAL_PRODUCT_PRES_STATUS]
    :create "create table VIRTUAL_PRODUCT_PRES_STATUS (CD integer primary key, DESC text)"
    :insert "insert into VIRTUAL_PRODUCT_PRES_STATUS(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :CONTROL_DRUG_CATEGORY]
    :create "create table CONTROL_DRUG_CATEGORY (CD integer primary key, DESC text)"
    :insert "insert into CONTROL_DRUG_CATEGORY(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :LICENSING_AUTHORITY]
    :create "create table LICENSING_AUTHORITY (CD integer primary key, DESC text)"
    :insert "insert into LICENSING_AUTHORITY(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :UNIT_OF_MEASURE]
    :create "create table UNIT_OF_MEASURE (CD integer primary key, DESC text)"
    :insert "insert into UNIT_OF_MEASURE(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :FORM]
    :create "create table FORM (CD integer primary key, DESC text)"
    :insert "insert into FORM(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :ONT_FORM_ROUTE]
    :create "create table ONT_FORM_ROUTE (CD integer primary key, DESC text)"
    :insert "insert into ONT_FORM_ROUTE(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :ROUTE]
    :create "create table ROUTE (CD integer primary key, DESC text)"
    :insert "insert into ROUTE(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :DT_PAYMENT_CATEGORY]
    :create "create table DT_PAYMENT_CATEGORY (CD integer primary key, DESC text)"
    :insert "insert into DT_PAYMENT_CATEGORY(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :SUPPLIER]
    :create "create table SUPPLIER (CD integer primary key, DESC text)"
    :insert "insert into SUPPLIER(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :FLAVOUR]
    :create "create table FLAVOUR (CD integer primary key, DESC text)"
    :insert "insert into FLAVOUR(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :COLOUR]
    :create "create table COLOUR (CD integer primary key, DESC text)"
    :insert "insert into COLOUR(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :BASIS_OF_STRNTH]
    :create "create table BASIS_OF_STRNTH (CD integer primary key, DESC text)"
    :insert "insert into BASIS_OF_STRNTH(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :REIMBURSEMENT_STATUS]
    :create "create table REIMBURSEMENT_STATUS (CD integer primary key, DESC text)"
    :insert "insert into REIMBURSEMENT_STATUS(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :SPEC_CONT]
    :create "create table SPEC_CONT (CD integer primary key, DESC text)"
    :insert "insert into SPEC_CONT(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :DND]
    :create "create table DND (CD integer primary key, DESC text)"
    :insert "insert into DND(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :VIRTUAL_PRODUCT_NON_AVAIL]
    :create "create table VIRTUAL_PRODUCT_NON_AVAIL (CD integer primary key, DESC text)"
    :insert "insert into VIRTUAL_PRODUCT_NON_AVAIL(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :DISCONTINUED_IND]
    :create "create table DISCONTINUED_IND (CD integer primary key, DESC text)"
    :insert "insert into DISCONTINUED_IND(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :DF_INDICATOR]
    :create "create table DF_INDICATOR (CD integer primary key, DESC text)"
    :insert "insert into DF_INDICATOR(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :PRICE_BASIS]
    :create "create table PRICE_BASIS (CD integer primary key, DESC text)"
    :insert "insert into PRICE_BASIS(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :LEGAL_CATEGORY]
    :create "create table LEGAL_CATEGORY (CD integer primary key, DESC text)"
    :insert "insert into LEGAL_CATEGORY(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :AVAILABILITY_RESTRICTION]
    :create "create table AVAILABILITY_RESTRICTION (CD integer primary key, DESC text)"
    :insert "insert into AVAILABILITY_RESTRICTION(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :LICENSING_AUTHORITY_CHANGE_REASON]
    :create "create table LICENSING_AUTHORITY_CHANGE_REASON (CD integer primary key, DESC text)"
    :insert "insert into LICENSING_AUTHORITY_CHANGE_REASON(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:INGREDIENT :INGREDIENT]
    :create "create table INGREDIENT (ISID integer primary key, NM text)"
    :insert "insert into INGREDIENT(ISID,NM) VALUES (?, ?) ON CONFLICT(ISID) DO UPDATE SET NM=excluded.NM"
    :data   (juxt :ISID :NM)}
   {:id     [:VTM :VTM]
    :create "create table VTM (VTMID integer primary key, INVALID boolean, NM text, ABBREVNM text, VTMIDPREV integer, VTMIDDT text)"
    :insert "insert into VTM(VTMID, INVALID, NM, ABBREVNM, VTMIDPREV, VTMIDDT) VALUES (?,?,?,?,?,?)"
    :data   (juxt :VTMID :INVALID :NM :ABBREVNM :VTMIDPREV :VTMIDDT)}
   {:id     [:VMP :VMP]
    :create "create table VMP (VPID integer primary key, INVALID boolean, VTMID integer, NM text, BASISCD integer, PRES_STATCD integer, SUG_F boolean, GLU_F boolean, PRES_F boolean, CFC_F boolean,
             NON_AVAILCD integer, DF_INDCD integer, UDFS text, UDFS_UOMCD integer, UNIT_DOSE_UOMCD,
             foreign key (VTMID) references VTM(VTMID), foreign key(BASISCD) references BASIS_OF_NAME(CD), foreign key(PRES_STATCD) references VIRTUAL_PRODUCT_PRES_STATUS(CD),
             foreign key (NON_AVAILCD) references VIRTUAL_PRODUCT_NON_AVAIL(CD), foreign key (DF_INDCD) references DF_INDICATOR(CD),
             foreign key (UDFS_UOMCD) references UNIT_OF_MEASURE(CD), foreign key(UNIT_DOSE_UOMCD) references UNIT_OF_MEASURE(CD))"
    :insert "insert into VMP(VPID, INVALID, VTMID, NM, BASISCD, PRES_STATCD, SUG_F, GLU_F, PRES_F, CFC_F, NON_AVAILCD, DF_INDCD, UDFS, UDFS_UOMCD, UNIT_DOSE_UOMCD) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    :data   (juxt :VPID :INVALID :VTMID :NM :BASISCD :PRES_STATCD :SUG_F :GLU_F :PRES_F :CFC_F :NON_AVAILCD :DF_INDCD :UDFS :UDFS_UOMCD :UNIT_DOSE_UOMCD)}
   {:id     [:VMP :VIRTUAL_PRODUCT_INGREDIENT]
    :create "create table VMP__VIRTUAL_PRODUCT_INGREDIENT (VPID integer, ISID integer, BASIS_STRNTCD integer, STRNT_NMRTR_VAL real, STRNT_NMRTR_UOMCD integer,
             foreign key(VPID) REFERENCES VMP(VPID),foreign key(ISID) REFERENCES INGREDIENT(ISID),
             FOREIGN KEY(BASIS_STRNTCD) REFERENCES BASIS_OF_STRNTH(CD),FOREIGN KEY(STRNT_NMRTR_UOMCD) REFERENCES UNIT_OF_MEASURE(CD))"
    :insert "insert into VMP__VIRTUAL_PRODUCT_INGREDIENT(VPID, ISID, BASIS_STRNTCD, STRNT_NMRTR_VAL, STRNT_NMRTR_UOMCD) VALUES (?, ?, ?, ?, ?)"
    :data   (juxt :VPID :ISID :BASIS_STRNTCD :STRNT_NMRTR_VAL :STRNT_NMRTR_UOMCD)}
   {:id     [:VMP :ONT_DRUG_FORM]
    :create "create table VMP__ONT_DRUG_FORM (VPID integer, FORMCD integer, FOREIGN KEY(VPID) REFERENCES VMP(VPID), FOREIGN KEY(FORMCD) REFERENCES ONT_FORM_ROUTE(CD))"
    :insert "insert into VMP__ONT_DRUG_FORM(VPID, FORMCD) VALUES (?, ?)"
    :data   (juxt :VPID :FORMCD)}
   {:id     [:VMP :DRUG_FORM]
    :create "create table VMP__DRUG_FORM (VPID integer, FORMCD integer, FOREIGN KEY(VPID) REFERENCES VMP(VPID), FOREIGN KEY(FORMCD) REFERENCES FORM(CD))"
    :insert "insert into VMP__DRUG_FORM(VPID, FORMCD) VALUES (?, ?)"
    :data   (juxt :VPID :FORMCD)}
   {:id     [:VMP :DRUG_ROUTE]
    :create "create table VMP__DRUG_ROUTE (VPID integer, ROUTECD integer, FOREIGN KEY(VPID) REFERENCES VMP(VPID), FOREIGN KEY(ROUTECD) REFERENCES ROUTE(CD))"
    :insert "insert into VMP__DRUG_ROUTE(VPID, ROUTECD) VALUES (?, ?)"
    :data   (juxt :VPID :ROUTECD)}
   {:id     [:VMP :CONTROL_DRUG_INFO]
    :create "create table VMP__CONTROL_DRUG_INFO (VPID integer, CATCD integer, FOREIGN KEY(VPID) REFERENCES VMP(VPID), FOREIGN KEY(CATCD) REFERENCES CONTROL_DRUG_CATEGORY(CD))"
    :insert "insert into VMP__CONTROL_DRUG_INFO(VPID, CATCD) VALUES (?, ?)"
    :data   (juxt :VPID :CATCD)}
   {:id     [:AMP :AMP]
    :create "create table AMP (APID integer primary key, VPID integer, NM text, DESC text, SUPPCD integer, LIC_AUTHCD integer, AVAIL_RESTRICTCD integer,
             FOREIGN KEY(VPID) REFERENCES VMP(VPID), FOREIGN KEY(SUPPCD) REFERENCES SUPPLIER(CD),
             FOREIGN KEY(LIC_AUTHCD) REFERENCES LICENSING_AUTHORITY(CD), FOREIGN KEY(AVAIL_RESTRICTCD) REFERENCES AVAILABILITY_RESTRICTION(CD))"
    :insert "insert into AMP(APID, VPID, NM, DESC, SUPPCD, LIC_AUTHCD, AVAIL_RESTRICTCD) VALUES (?, ?, ?, ?, ?, ?, ?)"
    :data   (juxt :APID :VPID :NM :DESC :SUPPCD :LIC_AUTHCD :AVAIL_RESTRICTCD)}
   {:id     [:AMP :AP_INGREDIENT]                           ;; excipients
    :create "create table AMP__AP_INGREDIENT (APID integer, ISID integer, STRNTH text, UOMCD integer,
             foreign key(APID) references AMP(APID), foreign key(ISID) references INGREDIENT(ISID), foreign key(UOMCD) references UNIT_OF_MEASURE(CD))"
    :insert "insert into AMP__AP_INGREDIENT(APID, ISID, STRNTH, UOMCD) values (?, ?, ?, ?)"
    :data   (juxt :APID :ISID, :STRNTH, :UOMCD)}
   {:id     [:AMP :LICENSED_ROUTE]
    :create "create table AMP__LICENSED_ROUTE (APID integer, ROUTECD integer, foreign key(APID) REFERENCES AMP(APID), FOREIGN KEY(ROUTECD) REFERENCES ROUTE(CD))"
    :insert "insert into AMP__LICENSED_ROUTE(APID, ROUTECD) VALUES (?, ?)"
    :data   (juxt :APID :ROUTECD)}
   {:id     [:AMP :AP_INFORMATION]
    :create "create table AMP__AP_INFORMATION (APID integer, PROD_ORDER_NO text, foreign key(APID) REFERENCES AMP(APID))"
    :insert "insert into AMP__AP_INFORMATION(APID, PROD_ORDER_NO) VALUES (?, ?)"
    :data   (juxt :APID :PROD_ORDER_NO)}
   {:id     [:VMPP :VMPP]
    :create "create table VMPP (VPPID integer primary key, INVALID integer, NM text, VPID integer, QTYVAL text, QTY_UOMCD integer, COMBPACKCD integer,
             foreign key(VPID) REFERENCES VMP(VPID), foreign key(QTY_UOMCD) REFERENCES UNIT_OF_MEASURE(CD),
             FOREIGN KEY(COMBPACKCD) REFERENCES COMBINATION_PACK_IND(CD))"
    :insert "insert into VMPP(VPPID, NM, VPID, QTYVAL, QTY_UOMCD) VALUES (?, ?, ?, ?, ?)"
    :data   (juxt :VPPID :NM :VPID :QTYVAL :QTY_UOMCD)}
   {:id     [:VMPP :DRUG_TARIFF_INFO]
    :create "create table VMPP__DRUG_TARIFF_INFO (VPPID integer, PAY_CATCD integer, PRICE text, DT text, PREVPRICE text,
             foreign key(VPPID) REFERENCES VMPP(VPPID), foreign key(PAY_CATCD) REFERENCES DT_PAYMENT_CATEGORY(CD))"
    :insert "insert into VMPP__DRUG_TARIFF_INFO(VPPID, PAY_CATCD, PRICE, DT, PREVPRICE) VALUES (?, ?, ?, ?, ?)"
    :data   (juxt :VPPID :PAY_CATCD :PRICE :DT :PREVPRICE)}
   {:id     [:VMPP :COMB_CONTENT]
    :create "create table VMPP__COMB_CONTENT (PRNTVPPID integer, CHLDVPPID integer,
             foreign key(PRNTVPPID) REFERENCES VMPP(VPPID),foreign key(CHLDVPPID) REFERENCES VMPP(VPPID))"
    :insert "insert into VMPP__COMB_CONTENT(PRNTVPPID, CHLDVPPID) VALUES (?, ?)"
    :data   (juxt :PRNTVPPID :CHLDVPPID)}
   {:id     [:AMPP :AMPP]
    :create "create table AMPP (APPID integer primary key, INVALID integer, NM text, ABBREVNM text,
             VPPID integer, APID integer, COMBPACKCD integer, LEGAL_CATCD integer,
             SUBP text, DISCCD integer,
             foreign key(VPPID) REFERENCES VMPP(VPPID), foreign key(APID) REFERENCES AMP(APID),
             FOREIGN KEY(COMBPACKCD) REFERENCES COMBINATION_PACK_IND(CD), foreign key(LEGAL_CATCD) REFERENCES LEGAL_CATEGORY(CD),
             foreign key(DISCCD) REFERENCES DISCONTINUED_IND(CD))"
    :insert "insert into AMPP(APPID, INVALID, NM, ABBREVNM, VPPID, APID, COMBPACKCD, LEGAL_CATCD, SUBP, DISCCD) VALUES (?, ?, ?, ?, ?, ?, ?,?,?,?)"
    :data   (juxt :APPID :INVALID :NM :ABBREVNM :VPPID :APID :COMBPACKCD :LEGAL_CATCD :SUBP :DISCCD)}
   {:id     [:AMPP :APPLIANCE_PACK_INFO]
    :create "create table AMPP__APPLIANCE_PACK_INFO (APPID integer, REIMB_STATCD integer, REIMB_STATDT text, REIMB_STATPREVCD integer, PACK_ORDER_NO integer,
             foreign key(APPID) REFERENCES AMPP(APPID), foreign key(REIMB_STATCD) REFERENCES REIMBURSEMENT_STATUS(CD)
             foreign key(REIMB_STATPREVCD) REFERENCES REIMBURSEMENT_STATUS(CD))"
    :insert "insert into AMPP__APPLIANCE_PACK_INFO (APPID, REIMB_STATCD, REIMB_STATDT, REIMB_STATPREVCD, PACK_ORDER_NO) VALUES (?,?,?,?,?)"
    :data   (juxt :APPID :REIMB_STATCD :REIMB_STATDT :REIMB_STATPREVCD :PACK_ORDER_NO)}
   {:id     [:AMPP :DRUG_PRODUCT_PRESCRIB_INFO]
    :create "create table AMPP__DRUG_PRODUCT_PRESCRIB_INFO (APPID integer, SCHED_2 integer, ACBS integer, PADM integer, FP10_MDA integer,
             SCHED_1 integer, HOSP integer, NURSE_F integer, ENURSE_F integer, DENT_F integer,
             foreign key(APPID) REFERENCES AMPP(APPID))"
    :insert "insert into AMPP__DRUG_PRODUCT_PRESCRIB_INFO (APPID, SCHED_2, ACBS, PADM, FP10_MDA, SCHED_1, HOSP, NURSE_F, ENURSE_F, DENT_F) VALUES (?,?,?,?,?,?,?,?,?,?)"
    :data   (juxt :APPID :SCHED_2 :ACBS :PADM :FP10_MDA :SCHED_1 :HOSP :NURSE_F :ENURSE_F :DENT_F)}
   {:id     [:AMPP :MEDICINAL_PRODUCT_PRICE]
    :create "create table AMPP__MEDICINAL_PRODUCT_PRICE (APPID integer, PRICE integer, PRICEDT text, PRICE_PREV integer, PRICE_BASISCD integer,
             foreign key(APPID) REFERENCES AMPP(APPID), foreign key(PRICE_BASISCD) REFERENCES PRICE_BASIS(CD))"
    :insert "insert into AMPP__MEDICINAL_PRODUCT_PRICE (APPID, PRICE, PRICEDT, PRICE_PREV, PRICE_BASISCD) VALUES (?,?,?,?,?)"
    :data   (juxt :APPID :PRICE :PRICEDT :PRICE_PREV :PRICE_BASISCD)}
   {:id     [:AMPP :REIMBURSEMENT_INFO]
    :create "create table AMPP__REIMBURSEMENT_INFO (APPID integer, PX_CHRGS integer, DISP_FEES integer, BB integer, CAL_PACK integer,
             SPEC_CONTCD integer, DND integer, FP34D integer,
             foreign key(APPID) REFERENCES AMPP(APPID), foreign key(SPEC_CONTCD) REFERENCES SPEC_CONT(CD))"
    :insert "insert into AMPP__REIMBURSEMENT_INFO (APPID, PX_CHRGS, DISP_FEES, BB, CAL_PACK, SPEC_CONTCD, DND, FP34D) VALUES (?,?,?,?,?,?,?,?)"
    :data   (juxt :APPID :PX_CHRGS :DISP_FEES :BB :CAL_PACK :SPEC_CONTCD :DND :FP34D)}
   {:id     [:AMPP :COMB_CONTENT]
    :create "create table AMPP__COMB_CONTENT (PRNTAPPID integer, CHLDAPPID integer,
             foreign key(PRNTAPPID) REFERENCES AMPP(APPID), foreign key(CHLDAPPID) REFERENCES AMPP(APPID))"
    :insert "insert into AMPP__COMB_CONTENT (PRNTAPPID, CHLDAPPID) VALUES (?,?)"
    :data   (juxt :PRNTAPPID :CHLDAPPID)}
   {:id     [:GTIN :AMPP]
    :create "create table GTIN__AMPP (AMPPID integer, GTIN string, STARTDT string)"
    :insert "insert into GTIN__AMPP (AMPPID, GTIN, STARTDT) VALUES (?,?,?)"
    :data   (juxt :AMPPID :GTIN :STARTDT)}
   {:id     [:BNF :VMPS]
    :create "create table BNF_DETAILS (VPID integer, BNF string, ATC string, DDD string, DDD_UOMCD integer,
             foreign key(DDD_UOMCD) references UNIT_OF_MEASURE(CD))"
    :insert "insert into BNF_DETAILS (VPID, BNF, ATC, DDD, DDD_UOMCD) VALUES (?,?,?,?,?)"
    :data   (juxt :VPID :BNF :ATC :DDD :DDD_UOMCD)}])

(when-not
 (s/valid? (s/coll-of ::entity) entities)
  (throw (ex-info "Invalid entity definition" (s/explain-data (s/coll-of ::entity) entities))))

(def entity-by-type
  (reduce (fn [acc {:keys [id] :as entity}] (assoc acc id entity)) {} entities))

(s/fdef create-tables
  :args (s/cat :conn ::conn))

(defn create-tables
  "Creates all database tables for dm+d entities."
  [conn]
  (run! #(jdbc/execute-one! conn [%]) (map :create entities)))

(s/fdef create-indexes
  :args (s/cat :conn ::conn))
(defn create-indexes
  [conn]
  (->> entities (map :index) (remove nil?) (run! #(jdbc/execute! conn [%]))))

(defn batch->sql
  "For the given batch of dm+d entities, return a map of :stmts and :errors
  ```
  {:stmts ({:stmt \"insert into SUPPLIER(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC\",
            :data  ([2070501000001104 \"DDC Ltd\"]
                    [3146101000001108 \"Dragon Pharm Ltd\"] ...)})}
   :errors nil"
  [batch]
  (reduce-kv
   (fn [acc entity-type batch]
     (if-let [{:keys [insert data]} (entity-by-type entity-type)]
       (update acc :stmts conj {:id entity-type, :stmt insert, :data (mapv data batch)})
       (do
         (println (str "skipping unsupported dm+d entity :" entity-type ": " (first batch)))
         (update acc :errors conj {:error :unsupported :type entity-type}))))
   {}
   (group-by :TYPE batch)))

(comment
  (compile 'com.eldrix.dmd.sqlite))

(defn open-store
  [filename]
  (if (.exists (io/file filename))
    (jdbc/get-connection (str "jdbc:sqlite:" filename))
    (throw (ex-info (str "file not found:" filename) {}))))

(defn close
  "Close the store."
  [^java.sql.Connection conn]
  (.close conn))

(s/fdef fetch-release-date
  :args (s/cat :conn ::conn))
(defn fetch-release-date
  [conn]
  (some-> (:METADATA/release (jdbc/execute-one! conn ["select release from metadata"])) LocalDate/parse))

(defn create-store
  [filename dirs & {:keys [batch-size release-date] :or {batch-size 50000}}]
  (when (.exists (io/file filename))
    (throw (ex-info (str "dm+d database already exists: " filename) {})))
  (let [conn (jdbc/get-connection (str "jdbc:sqlite:" filename))
        ch (async/chan 5 (comp (partition-all batch-size) (map batch->sql)))]
    (async/thread
      (doseq [dir dirs]
        (dim/stream-dmd dir ch :close? false))
      (async/close! ch))
    (create-tables conn)
    (jdbc/execute! conn ["insert into metadata (version, created, release) values (?,?,?)"
                         store-version (LocalDateTime/now) release-date])
    (try
      (loop [{:keys [stmts errors] :as batch} (async/<!! ch), all-errors #{}]
        (if-not batch
          (do (create-indexes conn)
              {:conn   conn
               :errors (seq all-errors)})
          (do
            (jdbc/with-transaction [txn conn]
              (doseq [{:keys [stmt data]} stmts]
                (jdbc/execute-batch! txn stmt data {})))
            (recur (async/<!! ch), (into all-errors errors)))))
      (catch Exception e
        (log/warn (ex-message e))
        {:errors e}))))

;;;
;;;
;;;
(s/fdef fetch-lookup
  :args (s/cat :conn ::conn :lookup ::lookup :code ::code))
(defn fetch-lookup [conn lookup code]
  (when code (jdbc/execute-one! conn [(str "select CD,DESC from " (name lookup) " WHERE CD=?") code])))

(s/fdef fetch-all-lookup
  :args (s/cat :conn ::conn :lookup ::lookup))
(defn fetch-all-lookup [conn lookup]
  (jdbc/execute! conn [(str "select CD,DESC from " (name lookup))]))

(s/fdef fetch-vtm*
  :args (s/cat :conn ::conn :vtmid ::vtmid))
(defn fetch-vtm*
  "Returns the given VTM with no extended information."
  [conn vtmid]
  (some-> (jdbc/execute-one! conn ["select * from vtm where vtmid=?" vtmid])
          (assoc :TYPE "VTM")))

(s/fdef fetch-vtm
  :args (s/cat :conn ::conn :vtmid ::vtmid))
(def fetch-vtm
  "Returns the given VTM. VTMs do not have extended information so this
  returns the same as `fetch-vtm*`."
  fetch-vtm*)

(s/fdef fetch-ingredient
  :args (s/cat :conn ::conn :isid ::isid))
(defn fetch-ingredient
  "Returns the ingredient specified."
  [conn isid]
  (jdbc/execute! conn ["select * from ingredient where isid=?" isid]))

(s/fdef fetch-vmp-ingredients
  :args (s/cat :conn ::conn :vpid ::vpid))
(defn ^:private fetch-vmp-ingredients
  "Returns ingredients for the given VMP"
  [conn vpid]
  (->> (jdbc/execute! conn ["select * from VMP__VIRTUAL_PRODUCT_INGREDIENT where VPID=?" vpid])
       (map (fn [{:VMP__VIRTUAL_PRODUCT_INGREDIENT/keys [ISID BASIS_STRNTCD STRNT_NMRTR_UOMCD] :as vpi}]
              (assoc vpi :VMP__VIRTUAL_PRODUCT_INGREDIENT/IS (fetch-ingredient conn ISID)
                     :VMP__VIRTUAL_PRODUCT_INGREDIENT/BASIS_STRNT (fetch-lookup conn :BASIS_OF_STRNTH BASIS_STRNTCD)
                     :VMP__VIRTUAL_PRODUCT_INGREDIENT/STRNT_NMRTR_UOM (fetch-lookup conn :UNIT_OF_MEASURE STRNT_NMRTR_UOMCD))))))

(s/fdef fetch-vmp-ont-drug-forms
  :args (s/cat :conn ::conn :vpid ::vpid))
(defn ^:private fetch-vmp-ont-drug-forms
  [conn vpid]
  (->> (jdbc/execute! conn ["select * from vmp__ont_drug_form where vpid=?" vpid])
       (map (fn [{:VMP__ONT_DRUG_FORM/keys [FORMCD] :as odf}]
              (assoc odf :VMP__ONT_DRUG_FORM/FORM (fetch-lookup conn :ONT_FORM_ROUTE FORMCD))))))

(s/fdef fetch-vmp-drug-form
  :args (s/cat :conn ::conn :vpid ::vpid))
(defn ^:private fetch-vmp-drug-form
  [conn vpid]
  (when-let [{:VMP__DRUG_FORM/keys [FORMCD]} (jdbc/execute-one! conn ["select * from vmp__drug_form where vpid=?" vpid])]
    (fetch-lookup conn :FORM FORMCD)))                      ;; flatten the relationship directly

(s/fdef fetch-vmp-drug-routes
  :args (s/cat :conn ::conn :vpid ::vpid))
(defn ^:private fetch-vmp-drug-routes
  [conn vpid]
  (->> (jdbc/execute! conn ["select * from vmp__drug_route where vpid=?" vpid])
       (map (fn [{:VMP__DRUG_ROUTE/keys [ROUTECD] :as dr}]
              (assoc dr :VMP__DRUG_ROUTE/FORM (fetch-lookup conn :ROUTE ROUTECD))))))

(s/fdef fetch-vmp-control-drug-info
  :args (s/cat :conn ::conn :vpid ::vpid))
(defn ^:private fetch-vmp-control-drug-info
  [conn vpid]
  (when-let [{:VMP__CONTROL_DRUG_INFO/keys [CATCD] :as cdi} (jdbc/execute-one! conn ["select * from VMP__CONTROL_DRUG_INFO where vpid=?" vpid])]
    (assoc cdi :VMP__CONTROL_DRUG_INFO/CAT (fetch-lookup conn :CONTROL_DRUG_CATEGORY CATCD))))

(s/fdef fetch-vmp-bnf-details
  :args (s/cat :conn ::conn :vpid ::vpid))
(defn ^:private fetch-vmp-bnf-details
  [conn vpid]
  (when-let [{:BNF_DETAILS/keys [DDD_UOMCD] :as bnf} (jdbc/execute-one! conn ["select * from BNF_DETAILS where vpid=?" vpid])]
    (assoc bnf :BNF_DETAILS/DDD_UOM (fetch-lookup conn :UNIT_OF_MEASURE DDD_UOMCD))))

(s/fdef fetch-vmp*
  :args (s/cat :conn ::conn :vpid ::vpid))
(defn fetch-vmp*
  "Return the given VMP without extended information."
  [conn vpid]
  (some-> (jdbc/execute-one! conn ["select * from vmp where vpid=?" vpid])
          (assoc :TYPE "VMP")))

(s/fdef fetch-vmp
  :args (s/cat :conn ::conn :vpid ::vpid))
(defn fetch-vmp
  "Return the given VMP with extended information to include its 
  relationships such as its parent VTM."
  [conn vpid]
  (when-let [{:VMP/keys [VTMID BASISCD COMBPRODCD PRES_STATCD DF_INDCD NON_AVAILCD UDFS_UOMCD UNIT_DOSE_UOMCD] :as vmp} (jdbc/execute-one! conn ["select * from vmp where vpid=?" vpid])]
    (assoc vmp
           :TYPE "VMP"
           :VMP/VTM (fetch-vtm conn VTMID)                       ;; to-one
           :VMP/BASIS (fetch-lookup conn :BASIS_OF_NAME BASISCD) ;; to-one
           :VMP/COMBPROD (fetch-lookup conn :COMBINATION_PROD_IND COMBPRODCD)
           :VMP/PRES_STAT (fetch-lookup conn :VIRTUAL_PRODUCT_PRES_STATUS PRES_STATCD)
           :VMP/NON_AVAIL (fetch-lookup conn :VIRTUAL_PRODUCT_NON_AVAIL NON_AVAILCD)
           :VMP/UDFS_UOM (fetch-lookup conn :UNIT_OF_MEASURE UDFS_UOMCD)
           :VMP/UNIT_DOSE_UOM (fetch-lookup conn :UNIT_OF_MEASURE UNIT_DOSE_UOMCD)
           :VMP/DF_IND (fetch-lookup conn :DF_INDICATOR DF_INDCD) ;; to-one
           :VMP/VIRTUAL_PRODUCT_INGREDIENTS (fetch-vmp-ingredients conn vpid) ;; to-many
           :VMP/ONT_DRUG_FORMS (fetch-vmp-ont-drug-forms conn vpid) ;; to-many
           :VMP/DRUG_FORM (fetch-vmp-drug-form conn vpid)        ;; to-one
           :VMP/DRUG_ROUTES (fetch-vmp-drug-routes conn vpid)    ;; to-many
           :VMP/CONTROL_DRUG_INFO (fetch-vmp-control-drug-info conn vpid)
           :VMP/BNF_DETAILS (fetch-vmp-bnf-details conn vpid)))) ;;to-one

(s/fdef fetch-vmpp*
  :args (s/cat :conn ::conn :vppid ::vppid))
(defn fetch-vmpp*
  "Return the given VMPP without extended information."
  [conn vppid]
  (some-> (jdbc/execute-one! conn ["select * from vmpp where vppid=?" vppid])
          (assoc :TYPE "VMPP")))

(s/fdef fetch-vmpp
  :args (s/cat :conn ::conn :vppid ::vppid))
(defn fetch-vmpp
  "Return the given VMPP with extended information, including its parent VMP."
  [conn vppid]
  (when-let [{:VMPP/keys [VPID COMBPACKCD QTY_UOMCD] :as vmpp}
             (jdbc/execute-one! conn ["select * from vmpp where vppid=?" vppid])]
    (assoc vmpp
           :TYPE "VMPP"
           :VMPP/VP (fetch-vmp conn VPID)
           :VMPP/QTY_UOM (fetch-lookup conn :UNIT_OF_MEASURE QTY_UOMCD)
           :VMPP/COMBPACK (fetch-lookup conn :COMBINATION_PACK_IND COMBPACKCD))))

(s/fdef fetch-amp*
  :args (s/cat :conn ::conn :apid ::apid))
(defn fetch-amp*
  "Return the given AMP without extended information."
  [conn apid]
  (some-> (jdbc/execute-one! conn ["select * from amp where apid=?" apid])
          (assoc :TYPE "AMP")))

(s/fdef fetch-amp
  :args (s/cat :conn ::conn :apid ::apid))
(defn fetch-amp
  "Return the given AMP with extended information, including the parent VMP."
  [conn apid]
  (when-let [{:AMP/keys [VPID SUPPCD LIC_AUTHCD AVAIL_RESTRICTCD] :as amp}
             (jdbc/execute-one! conn ["select * from amp where apid=?" apid])]
    (assoc amp
           :TYPE "AMP"
           :AMP/SUPP (fetch-lookup conn :SUPPLIER SUPPCD)
           :AMP/LIC_AUTH (fetch-lookup conn :LICENSING_AUTHORITY LIC_AUTHCD)
           :AMP/AVAIL_RESTRICT (fetch-lookup conn :AVAILABILITY_RESTRICTION AVAIL_RESTRICTCD)
           :AMP/VP (fetch-vmp conn VPID))))

(s/fdef fetch-ampp*
  :args (s/cat :conn ::conn :appid ::appid))
(defn fetch-ampp*
  "Return the given AMPP without extended information."
  [conn appid]
  (some-> (jdbc/execute-one! conn ["select * from ampp where appid=?" appid])
          (assoc :TYPE "AMPP")))

(s/fdef fetch-ampp
  :args (s/cat :conn ::conn :appid ::appid))
(defn fetch-ampp
  "Return the given AMPP with extended information, including the parent AMP 
  and VMP."
  [conn appid]
  (when-let [{:AMPP/keys [VPPID APID COMBPACKCD LEGAL_CATCD DISCCD] :as ampp}
             (jdbc/execute-one! conn ["select * from ampp where appid=?" appid])]
    (assoc ampp
           :TYPE "AMPP"
           :AMPP/AP (fetch-amp conn APID)
           :AMPP/VPP (fetch-vmpp conn VPPID)
           :AMPP/COMBPACK (fetch-lookup conn :COMBINATION_PACK_IND COMBPACKCD)
           :AMPP/LEGAL_CAT (fetch-lookup conn :LEGAL_CATEGORY LEGAL_CATCD)
           :AMPP/DISC (fetch-lookup conn :DISCONTINUED_IND DISCCD))))

(s/fdef fetch-product
  :args (s/cat :conn ::conn :id ::product-id))
(defn fetch-product
  "Returns extended information about the product with identifier specified."
  [conn id]
  (or (fetch-vtm conn id)
      (fetch-vmp conn id)
      (fetch-amp conn id)
      (fetch-vmpp conn id)
      (fetch-ampp conn id)))

(s/fdef product-type
  :args (s/cat :conn ::conn :id ::product-id))
(defn product-type
  "Returns a keyword :VTM :VMP :AMP :VMPP or :AMPP for the product with 
  the identifier specified."
  [conn id]
  (keyword
   (:type
    (or
     (jdbc/execute-one! conn ["select 'VTM' as type,vtmid from vtm where vtmid=?" id])
     (jdbc/execute-one! conn ["select 'VMP' as type,vpid from vmp where vpid=?" id])
     (jdbc/execute-one! conn ["select 'AMP' as type,apid from amp where apid=?" id])
     (jdbc/execute-one! conn ["select 'VMPP' as type,vppid from vmpp where vppid=?" id])
     (jdbc/execute-one! conn ["select 'AMPP' as type,appid from ampp where appid=?" id])))))

(s/fdef fetch-product-by-exact-name
  :args (s/cat :conn ::conn :s string?))
(defn fetch-product-by-exact-name
  "Return a single product with the given exact name"
  [conn s]
  (when-let [x (or (jdbc/execute-one! conn ["select * from vtm where nm=?" s])
                   (jdbc/execute-one! conn ["select * from vmp where nm=?" s])
                   (jdbc/execute-one! conn ["select * from amp where nm=?" s])
                   (jdbc/execute-one! conn ["select * from vmpp where nm=?" s])
                   (jdbc/execute-one! conn ["select * from ampp where nm=?" s]))]
    (let [nspace (-> (keys x) first namespace)]
      (assoc x :TYPE nspace))))

(defn ^:private atc->like [s]
  (-> s (str/replace "*" "%") (str/replace "?" "_")))

(s/fdef vpids-from-atc
  :args (s/cat :conn ::conn :atc ::atc))
(defn vpids-from-atc
  "Return a vector of VPIDs matching the given ATC code."
  [conn atc]
  (into [] (map :VPID)
        (jdbc/plan conn ["select vpid from BNF_DETAILS where atc like ?" (atc->like atc)])))

(comment
  (def conn (jdbc/get-connection "jdbc:sqlite:latest.db"))
  (vpids-from-atc conn "C08CA")
  (vpids-from-atc conn (-> "C0?CA*" (str/replace "*" "%") (str/replace "?" "_"))))

(s/fdef vpids-from-atc-wo-vtms
  :args (s/cat :conn ::conn :atc ::atc))
(defn vpids-from-atc-wo-vtms
  "Return a vector of VPIDs matching the given ATC code/prefix that do not
  have an associated VTM. This is only useful when constructing SNOMED ECL
  expressions that use a combination of VTMs, VMPs and TFs, and therefore do
  not need VMPs unless there is no associated VTM."
  [conn atc]
  (into [] (map :VPID)
        (jdbc/plan conn ["select vpid from vmp where vtmid is null and vpid in (select vpid from BNF_DETAILS where atc like ?)" (atc->like atc)])))

(s/fdef vmps-from-atc
  :args (s/cat :conn ::conn :atc ::atc))
(defn ^:deprecated vmps-from-atc
  "Return VMPs matching the given ATC code as a prefix."
  [conn atc]
  (into [] (map #(fetch-vmp conn (:VPID %)))
        (jdbc/plan conn ["select vpid from BNF_DETAILS where atc like ?" (atc->like atc)])))

(s/fdef vpids-for-vtmids
  :args (s/cat :conn ::conn :vtmids (s/coll-of ::vtmid)))
(defn vpids-for-vtmids
  "Return VPIDs for the given VTMIDs."
  [conn vtmids]
  (into [] (map :VPID)
        (jdbc/plan conn (sql/format {:select :vpid :from :vmp :where [:in :vtmid vtmids]}))))

(s/fdef vtmids-for-vpids
  :args (s/cat :conn ::conn :vpids (s/coll-of ::vpid)))
(defn vtmids-for-vpids
  "Return VTMIDs for the given VPIDs."
  [conn vpids]
  (into #{} (comp (map :VTMID) (remove nil?))
        (jdbc/plan conn (sql/format {:select :vtmid :from :vmp :where [:in :vpid vpids]}))))

(s/fdef vppids-for-vpids
  :args (s/cat :conn ::conn :vpids (s/coll-of ::vpid)))
(defn vppids-for-vpids
  [conn vpids]
  (into [] (map :VPPID)
        (jdbc/plan conn (sql/format {:select :vppid :from :vmpp :where [:in :vpid vpids]}))))

(s/fdef vpids-for-vmpps
  :args (s/cat :conn ::conn :vppids (s/coll-of ::vpid)))
(defn vpids-for-vmpps
  [conn vppids]
  (into [] (map :VPID)
        (jdbc/plan conn (sql/format {:select :vpid :from :vmpp :where [:in :vppid vppids]}))))

(s/fdef vpids-for-vmpps
  :args (s/cat :conn ::conn :vppids (s/coll-of ::vpid)))
(defn apids-for-vpids
  "Return APIDs for the given VPIDs."
  [conn vpids]
  (into [] (map :APID)
        (jdbc/plan conn (sql/format {:select :apid :from :amp :where [:in :vpid vpids]}))))

(s/fdef vpids-for-apids
  :args (s/cat :conn ::conn :apids (s/coll-of ::apid)))
(defn vpids-for-apids
  "Return VPIDs for the given APIDs."
  [conn apids]
  (into [] (map :VPID)
        (jdbc/plan conn (sql/format {:select :vpid :from :amp :where [:in :apid apids]}))))

(s/fdef appids-for-apids
  :args (s/cat :conn ::conn :apids (s/coll-of ::apid)))
(defn appids-for-apids
  [conn apids]
  (into [] (map :APPID)
        (jdbc/plan conn (sql/format {:select :appid :from :ampp :where [:in :apid apids]}))))

(s/fdef apids-for-appids
  :args (s/cat :conn ::conn :appids (s/coll-of ::appid)))
(defn apids-for-appids
  [conn appids]
  (into [] (map :APID)
        (jdbc/plan conn (sql/format {:select :apid :from :ampp :where [:in :appid appids]}))))

(s/fdef appids-for-vppids
  :args (s/cat :conn ::conn :vppids (s/coll-of :vppid)))
(defn appids-for-vppids
  [conn vppids]
  (into [] (map :APPID)
        (jdbc/plan conn (sql/format {:select :appid :from :ampp :where [:in :vppid vppids]}))))

(s/fdef vppids-for-appids
  :args (s/cat :conn ::conn :appids (s/coll-of :appid)))
(defn vppids-for-appids
  [conn appids]
  (into [] (map :VPPID)
        (jdbc/plan conn (sql/format {:select :vppid :from :ampp :where [:in :appid appids]}))))

(s/fdef vpids
  :args (s/cat :conn ::conn :id ::product-id))
(defn vpids
  "Return VMP ids for the given product."
  [conn id]
  (case (product-type conn id)
    :VTM (vpids-for-vtmids conn [id])
    :VMP [id]
    :VMPP (vpids-for-vmpps conn [id])
    :AMP (vpids-for-apids conn [id])
    :AMPP (vpids-for-apids conn (apids-for-appids conn [id]))
    nil))

(s/fdef vtmids
  :args (s/cat :conn ::conn :id ::product-id))
(defn vtmids
  "Return VTM ids for the given product."
  [conn id]
  (case (product-type conn id)
    :VTM [id]
    :VMP (vtmids-for-vpids conn [id])
    :VMPP (vtmids-for-vpids conn (vpids-for-vmpps conn [id]))
    :AMP (vtmids-for-vpids conn (vpids-for-apids conn [id]))
    :AMPP (vtmids-for-vpids conn (apids-for-appids conn [id]))
    nil))

(s/fdef apids
  :args (s/cat :conn ::conn :id ::product-id))
(defn apids
  "Return AMP ids for the given product."
  [conn id]
  (case (product-type conn id)
    :VTM (apids-for-vpids conn (vpids-for-vtmids conn [id]))
    :VMP (apids-for-vpids conn [id])
    :VMPP (apids-for-vpids conn (vpids-for-apids conn [id]))
    :AMP [id]
    :AMPP (apids-for-appids conn [id])
    nil))

(s/fdef vppids
  :args (s/cat :conn ::conn :id ::product-id))
(defn vppids
  "Return VMPP ids for the given product."
  [conn id]
  (case (product-type conn id)
    :VTM (vppids-for-vpids conn (vpids-for-vtmids conn [id]))
    :VMP (vppids-for-vpids conn [id])
    :VMPP [id]
    :AMP (vppids-for-vpids conn (vpids-for-apids conn [id]))
    :AMPP (vppids-for-appids conn [id])
    nil))

(s/fdef appids
  :args (s/cat :conn :conn :id ::product-id))
(defn appids
  "Return AMPP ids for the given product."
  [conn id]
  (case (product-type conn id)
    :VTM (appids-for-apids conn (apids-for-vpids conn (vpids-for-vtmids conn [id])))
    :VMP (appids-for-apids conn (apids-for-vpids conn [id]))
    :VMPP (appids-for-vppids conn [id])
    :AMP (appids-for-apids conn [id])
    :AMPP [id]
    nil))

(s/fdef atc-code-for-vpids
  :args (s/cat :conn ::conn :vpids (s/coll-of ::vpid)))
(defn atc-code-for-vpids
  [conn vpids]
  (:BNF_DETAILS/ATC (jdbc/execute-one! conn (sql/format {:select :atc :from :BNF_DETAILS :where [:and [:<> :atc nil] [:in :vpid vpids]] :limit 1}))))

(s/fdef atc-code
  :args (s/cat :conn ::conn :id ::product-id))
(defn atc-code
  "Return the ATC code for the product specified."
  [conn id]
  (case (product-type conn id)
    :VTM (atc-code-for-vpids conn (vpids-for-vtmids conn [id]))
    :VMP (:BNF_DETAILS/ATC (jdbc/execute-one! conn ["select ATC from BNF_DETAILS where VPID=?" id]))
    :AMP (atc-code-for-vpids conn (vpids-for-apids conn [id]))
    :VMPP (atc-code-for-vpids conn (vpids-for-vmpps conn [id]))
    :AMPP (atc-code-for-vpids conn (vpids-for-apids conn (apids-for-appids conn [id])))
    nil))

(def supported-product-types-for-atc-map
  #{:VTM :VMP :AMP :VMPP :AMPP})

(s/fdef product-ids-from-atc
  :args (s/cat :conn ::conn :atc ::atc :product-types (s/? supported-product-types-for-atc-map)))
(defn product-ids-from-atc
  "Return a lazy sequence of product ids matching the ATC code"
  ([conn atc]
   (product-ids-from-atc conn atc supported-product-types-for-atc-map))
  ([conn atc product-types]
   (when-not (set/subset? product-types supported-product-types-for-atc-map)
     (throw (ex-info "unsupported product-types for ATC mapping" {:requested product-types :supported supported-product-types-for-atc-map})))
   (let [vpids (vpids-from-atc conn atc)
         vtmids (when (product-types :VTM) (vtmids-for-vpids conn vpids))
         apids (when (or (product-types :AMP) (product-types :AMPP)) (apids-for-vpids conn vpids))
         vppids (when (product-types :VMPP) (vppids-for-vpids conn vpids))
         appids (when (product-types :AMPP) (appids-for-apids conn apids))]
     (concat (when (contains? product-types :VMP) vpids)
             vtmids
             (when (product-types :AMP) apids)
             vppids
             appids))))
(s/fdef atc->products-for-ecl
  :args (s/cat :conn ::conn :atc ::atc))
(defn atc->products-for-ecl
  "Returns a map containing product type as key and a sequence of product
  identifiers as each value, designed for building an ECL expression.
  Parameters:
  - conn : database connection
  - atc  : a string representing the ATC code, or prefix. 

  As the child relationships of a VTM include all VMPs and AMPs, we do not have
  to include VMPs or AMPs unless there is no VTM for a given VMP. As such, VMPs
  are only returned iff there is no associated VTM. However, all AMPs are
  returned as it is likely that those will be needed in order to derive a list
  of TF products. It is sadly the case that the stock dm+d does not include TF
  products, while the SNOMED drug extension does include those products."
  [conn atc]
  (let [vpids (vpids-from-atc conn atc)
        vpids' (vpids-from-atc-wo-vtms conn atc)]
    {:VTM (vtmids-for-vpids conn vpids)
     :VMP vpids'
     :AMP (apids-for-vpids conn vpids')}))

(defn ^:deprecated atc->ecl
  "DEPRECATED: use `atc->products-for-ecl` instead and use that data to build
  an ECL expression outside of this library.
  
  Convert an ATC code into a SNOMED CT expression that will identify all
  dm+d products relating to that code. It is almost always better to build an
  ECL expression using `atc->products-for-ecl` rather than this function.

  Not all VMPs have a VTM, but a given VTM will subsume all VTMs, VMPs and AMPs
  in the SNOMED drug model.

  Unfortunately, while the UK SNOMED drug extension includes trade family
  entities, dm+d does not. This is unfortunate. In order to identify the TF
  concept for any given AMP, we can use an ECL expression of the form
  (>'amp-concept-id' AND <9191801000001103|Trade Family|) to identify parent
  concepts in the hierarchy up to and not including the TF concept itself.
  However, this can result in a very long expression indeed. If you need to
  build an ECL expression that includes TF, this is better done within the
  context of the SNOMED drug extension. You can use 'atc->products-for-ecl'
  to help build that ECL expression.

  It would not be usual to want to include VMPP or AMPP, but you can include
  if required. All AMPPs are subsumed by VMPPs, so we simply add clauses to
  include VMPPs and descendants for each VMP using the 'Has VMP' relationship."
  [conn atc & {:keys [include-tf? include-product-packs?] :or {include-tf? false include-product-packs? false}}]
  (let [vmps (map #(str "<<" %) (vpids-from-atc-wo-vtms conn atc)) ;; this will only include VMPs without a VTM
        vpids (vpids-from-atc conn atc)
        vtms (map #(str "<<" %) (vtmids-for-vpids conn vpids))
        apids (apids-for-vpids conn vpids)
        ;; for TFs, we ask for Trade family children that are parents of each AMP:
        ;; one can build a much more optimised clause here if you have access to SNOMED drug extension
        tfs (when include-tf? (map (fn [apid] (str "<<(>" apid " AND <9191801000001103)")) apids))
        ;; for product-packs, we ask for UK products that 'Has VMP' of all VMPs we matched:
        pp (when include-product-packs? (map (fn [vpid] (str "<<(<8653601000001108:10362601000001103=" vpid ")")) vpids))]
    (str/join " OR " (concat vmps vtms tfs pp))))

(comment
  (def conn (jdbc/get-connection "jdbc:sqlite:wibble.db"))
  (def conn (open-store "dmd-2024-01-29.db"))
  (def conn (let [{:keys [conn errors]} (create-store "wibble.db" ["/Users/mark/Dev/trud"] {:release-date (LocalDateTime/now)})] conn)))
