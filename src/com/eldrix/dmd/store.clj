(ns com.eldrix.dmd.store
  "dm+d storage using sqlite"
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.eldrix.dmd.import :as dim]
            [honey.sql :as sql]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as connection])
  (:import (com.zaxxer.hikari HikariDataSource)
           (java.time LocalDate LocalDateTime)))

(set! *warn-on-reflection* true)

(def store-version 2)

;; SQLite application_id (file-header magic) identifying this as a dm+d store.
;; ASCII "dm+d" = 0x646D2B64. Treat as a fixed format-family identifier; do
;; not change. Schema evolution is signalled via store-version (user_version).
(def application-id 0x646D2B64)

(s/def ::conn any?)
(s/def ::id (s/or :kw keyword? :coll (s/coll-of keyword?)))
(s/def ::create string?)
(s/def ::insert string?)
(s/def ::data fn?)
(s/def ::index (s/or :str string? :coll (s/coll-of string?)))
(s/def ::entity (s/keys :req-un [::id ::create] :opt-un [::insert ::data ::index]))

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
    :create "create table METADATA (version integer, created text, release text, trud text)"}
   {:id     :files
    :create "create table FILES (type text, name text, date text)"}
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
    :create "create table UNIT_OF_MEASURE (CD integer primary key, CDDT text, CDPREV integer, DESC text)"
    :insert "insert into UNIT_OF_MEASURE(CD,CDDT,CDPREV,DESC) VALUES (?,?,?,?) ON CONFLICT(CD) DO UPDATE SET CDDT=excluded.CDDT, CDPREV=excluded.CDPREV, DESC=excluded.DESC"
    :data   (juxt :CD :CDDT :CDPREV :DESC)}
   {:id     [:LOOKUP :FORM]
    :create "create table FORM (CD integer primary key, CDDT text, CDPREV integer, DESC text)"
    :insert "insert into FORM(CD,CDDT,CDPREV,DESC) VALUES (?,?,?,?) ON CONFLICT(CD) DO UPDATE SET CDDT=excluded.CDDT, CDPREV=excluded.CDPREV, DESC=excluded.DESC"
    :data   (juxt :CD :CDDT :CDPREV :DESC)}
   {:id     [:LOOKUP :ONT_FORM_ROUTE]
    :create "create table ONT_FORM_ROUTE (CD integer primary key, DESC text)"
    :insert "insert into ONT_FORM_ROUTE(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :ROUTE]
    :create "create table ROUTE (CD integer primary key, CDDT text, CDPREV integer, DESC text)"
    :insert "insert into ROUTE(CD,CDDT,CDPREV,DESC) VALUES (?,?,?,?) ON CONFLICT(CD) DO UPDATE SET CDDT=excluded.CDDT, CDPREV=excluded.CDPREV, DESC=excluded.DESC"
    :data   (juxt :CD :CDDT :CDPREV :DESC)}
   {:id     [:LOOKUP :DT_PAYMENT_CATEGORY]
    :create "create table DT_PAYMENT_CATEGORY (CD integer primary key, DESC text)"
    :insert "insert into DT_PAYMENT_CATEGORY(CD,DESC) VALUES (?, ?) ON CONFLICT(CD) DO UPDATE SET DESC=excluded.DESC"
    :data   (juxt :CD :DESC)}
   {:id     [:LOOKUP :SUPPLIER]
    :create "create table SUPPLIER (CD integer primary key, CDDT text, CDPREV integer, INVALID boolean, DESC text)"
    :insert "insert into SUPPLIER(CD,CDDT,CDPREV,INVALID,DESC) VALUES (?,?,?,?,?) ON CONFLICT(CD) DO UPDATE SET CDDT=excluded.CDDT, CDPREV=excluded.CDPREV, INVALID=excluded.INVALID, DESC=excluded.DESC"
    :data   (juxt :CD :CDDT :CDPREV :INVALID :DESC)}
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
    :create "create table INGREDIENT (ISID integer primary key, ISIDDT text, ISIDPREV integer, INVALID boolean, NM text)"
    :insert "insert into INGREDIENT(ISID,ISIDDT,ISIDPREV,INVALID,NM) VALUES (?,?,?,?,?) ON CONFLICT(ISID) DO UPDATE SET ISIDDT=excluded.ISIDDT, ISIDPREV=excluded.ISIDPREV, INVALID=excluded.INVALID, NM=excluded.NM"
    :data   (juxt :ISID :ISIDDT :ISIDPREV :INVALID :NM)}
   {:id     [:VTM :VTM]
    :create "create table VTM (VTMID integer primary key, INVALID boolean, NM text, ABBREVNM text, VTMIDPREV integer, VTMIDDT text)"
    :insert "insert into VTM(VTMID, INVALID, NM, ABBREVNM, VTMIDPREV, VTMIDDT) VALUES (?,?,?,?,?,?)"
    :data   (juxt :VTMID :INVALID :NM :ABBREVNM :VTMIDPREV :VTMIDDT)}
   {:id     [:VMP :VMP]
    :create "create table VMP (VPID integer primary key, VPIDDT text, VPIDPREV integer, INVALID boolean, VTMID integer, NM text, ABBREVNM text,
             BASISCD integer, NMDT text, NMPREV text, BASIS_PREVCD integer, NMCHANGECD integer, COMBPRODCD integer,
             PRES_STATCD integer, SUG_F boolean, GLU_F boolean, PRES_F boolean, CFC_F boolean,
             NON_AVAILCD integer, NON_AVAILDT text, DF_INDCD integer, UDFS text, UDFS_UOMCD integer, UNIT_DOSE_UOMCD integer,
             foreign key (VTMID) references VTM(VTMID), foreign key(BASISCD) references BASIS_OF_NAME(CD), foreign key(PRES_STATCD) references VIRTUAL_PRODUCT_PRES_STATUS(CD),
             foreign key (BASIS_PREVCD) references BASIS_OF_NAME(CD), foreign key(NMCHANGECD) references NAMECHANGE_REASON(CD), foreign key(COMBPRODCD) references COMBINATION_PROD_IND(CD),
             foreign key (NON_AVAILCD) references VIRTUAL_PRODUCT_NON_AVAIL(CD), foreign key (DF_INDCD) references DF_INDICATOR(CD),
             foreign key (UDFS_UOMCD) references UNIT_OF_MEASURE(CD), foreign key(UNIT_DOSE_UOMCD) references UNIT_OF_MEASURE(CD))"
    :index  "create index VMP_VTMID_IDX on VMP(VTMID)"
    :insert "insert into VMP(VPID, VPIDDT, VPIDPREV, INVALID, VTMID, NM, ABBREVNM, BASISCD, NMDT, NMPREV, BASIS_PREVCD, NMCHANGECD, COMBPRODCD, PRES_STATCD, SUG_F, GLU_F, PRES_F, CFC_F, NON_AVAILCD, NON_AVAILDT, DF_INDCD, UDFS, UDFS_UOMCD, UNIT_DOSE_UOMCD) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    :data   (juxt :VPID :VPIDDT :VPIDPREV :INVALID :VTMID :NM :ABBREVNM :BASISCD :NMDT :NMPREV :BASIS_PREVCD :NMCHANGECD :COMBPRODCD :PRES_STATCD :SUG_F :GLU_F :PRES_F :CFC_F :NON_AVAILCD :NON_AVAILDT :DF_INDCD :UDFS :UDFS_UOMCD :UNIT_DOSE_UOMCD)}
   {:id     [:VMP :VIRTUAL_PRODUCT_INGREDIENT]
    :create "create table VMP__VIRTUAL_PRODUCT_INGREDIENT (VPID integer, ISID integer, BASIS_STRNTCD integer, BS_SUBID integer, STRNT_NMRTR_VAL real, STRNT_NMRTR_UOMCD integer, STRNT_DNMTR_VAL real, STRNT_DNMTR_UOMCD integer,
             foreign key(VPID) REFERENCES VMP(VPID),foreign key(ISID) REFERENCES INGREDIENT(ISID),
             FOREIGN KEY(BASIS_STRNTCD) REFERENCES BASIS_OF_STRNTH(CD),FOREIGN KEY(STRNT_NMRTR_UOMCD) REFERENCES UNIT_OF_MEASURE(CD),FOREIGN KEY(STRNT_DNMTR_UOMCD) REFERENCES UNIT_OF_MEASURE(CD))"
    :index  "create index VMP__VIRTUAL_PRODUCT_INGREDIENT_VPID_IDX on VMP__VIRTUAL_PRODUCT_INGREDIENT(VPID)"
    :insert "insert into VMP__VIRTUAL_PRODUCT_INGREDIENT(VPID, ISID, BASIS_STRNTCD, BS_SUBID, STRNT_NMRTR_VAL, STRNT_NMRTR_UOMCD, STRNT_DNMTR_VAL, STRNT_DNMTR_UOMCD) VALUES (?,?,?,?,?,?,?,?)"
    :data   (juxt :VPID :ISID :BASIS_STRNTCD :BS_SUBID :STRNT_NMRTR_VAL :STRNT_NMRTR_UOMCD :STRNT_DNMTR_VAL :STRNT_DNMTR_UOMCD)}
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
    :create "create table VMP__CONTROL_DRUG_INFO (VPID integer, CATCD integer, CATDT text, CAT_PREVCD integer,
             FOREIGN KEY(VPID) REFERENCES VMP(VPID), FOREIGN KEY(CATCD) REFERENCES CONTROL_DRUG_CATEGORY(CD), FOREIGN KEY(CAT_PREVCD) REFERENCES CONTROL_DRUG_CATEGORY(CD))"
    :insert "insert into VMP__CONTROL_DRUG_INFO(VPID, CATCD, CATDT, CAT_PREVCD) VALUES (?,?,?,?)"
    :data   (juxt :VPID :CATCD :CATDT :CAT_PREVCD)}
   {:id     [:AMP :AMP]
    :create "create table AMP (APID integer primary key, INVALID boolean, VPID integer, NM text, ABBREVNM text, DESC text, NMDT text, NM_PREV text,
             SUPPCD integer, LIC_AUTHCD integer, LIC_AUTH_PREVCD integer, LIC_AUTHCHANGECD integer, LIC_AUTHCHANGEDT text,
             COMBPRODCD integer, FLAVOURCD integer, EMA boolean, PARALLEL_IMPORT boolean, AVAIL_RESTRICTCD integer,
             FOREIGN KEY(VPID) REFERENCES VMP(VPID), FOREIGN KEY(SUPPCD) REFERENCES SUPPLIER(CD),
             FOREIGN KEY(LIC_AUTHCD) REFERENCES LICENSING_AUTHORITY(CD), FOREIGN KEY(LIC_AUTH_PREVCD) REFERENCES LICENSING_AUTHORITY(CD),
             FOREIGN KEY(LIC_AUTHCHANGECD) REFERENCES LICENSING_AUTHORITY_CHANGE_REASON(CD), FOREIGN KEY(COMBPRODCD) REFERENCES COMBINATION_PROD_IND(CD),
             FOREIGN KEY(FLAVOURCD) REFERENCES FLAVOUR(CD), FOREIGN KEY(AVAIL_RESTRICTCD) REFERENCES AVAILABILITY_RESTRICTION(CD))"
    :index  "create index AMP_VPID_IDX on AMP(VPID)"
    :insert "insert into AMP(APID, INVALID, VPID, NM, ABBREVNM, DESC, NMDT, NM_PREV, SUPPCD, LIC_AUTHCD, LIC_AUTH_PREVCD, LIC_AUTHCHANGECD, LIC_AUTHCHANGEDT, COMBPRODCD, FLAVOURCD, EMA, PARALLEL_IMPORT, AVAIL_RESTRICTCD) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    :data   (juxt :APID :INVALID :VPID :NM :ABBREVNM :DESC :NMDT :NM_PREV :SUPPCD :LIC_AUTHCD :LIC_AUTH_PREVCD :LIC_AUTHCHANGECD :LIC_AUTHCHANGEDT :COMBPRODCD :FLAVOURCD :EMA :PARALLEL_IMPORT :AVAIL_RESTRICTCD)}
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
    :create "create table AMP__AP_INFORMATION (APID integer, SZ_WEIGHT text, COLOURCD integer, PROD_ORDER_NO text,
             foreign key(APID) REFERENCES AMP(APID), foreign key(COLOURCD) REFERENCES COLOUR(CD))"
    :insert "insert into AMP__AP_INFORMATION(APID, SZ_WEIGHT, COLOURCD, PROD_ORDER_NO) VALUES (?,?,?,?)"
    :data   (juxt :APID :SZ_WEIGHT :COLOURCD :PROD_ORDER_NO)}
   {:id     [:VMPP :VMPP]
    :create "create table VMPP (VPPID integer primary key, INVALID boolean, NM text, ABBREVNM text, VPID integer, QTYVAL text, QTY_UOMCD integer, COMBPACKCD integer,
             foreign key(VPID) REFERENCES VMP(VPID), foreign key(QTY_UOMCD) REFERENCES UNIT_OF_MEASURE(CD),
             FOREIGN KEY(COMBPACKCD) REFERENCES COMBINATION_PACK_IND(CD))"
    :index  "create index VMPP_VPID_IDX on VMPP(VPID)"
    :insert "insert into VMPP(VPPID, INVALID, NM, ABBREVNM, VPID, QTYVAL, QTY_UOMCD, COMBPACKCD) VALUES (?,?,?,?,?,?,?,?)"
    :data   (juxt :VPPID :INVALID :NM :ABBREVNM :VPID :QTYVAL :QTY_UOMCD :COMBPACKCD)}
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
    :create "create table AMPP (APPID integer primary key, INVALID boolean, NM text, ABBREVNM text,
             VPPID integer, APID integer, COMBPACKCD integer, LEGAL_CATCD integer,
             SUBP text, DISCCD integer, DISCDT text,
             foreign key(VPPID) REFERENCES VMPP(VPPID), foreign key(APID) REFERENCES AMP(APID),
             FOREIGN KEY(COMBPACKCD) REFERENCES COMBINATION_PACK_IND(CD), foreign key(LEGAL_CATCD) REFERENCES LEGAL_CATEGORY(CD),
             foreign key(DISCCD) REFERENCES DISCONTINUED_IND(CD))"
    :index  ["create index AMPP_VPPID_IDX on AMPP(VPPID)"
             "create index AMPP_APID_IDX on AMPP(APID)"]
    :insert "insert into AMPP(APPID, INVALID, NM, ABBREVNM, VPPID, APID, COMBPACKCD, LEGAL_CATCD, SUBP, DISCCD, DISCDT) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
    :data   (juxt :APPID :INVALID :NM :ABBREVNM :VPPID :APID :COMBPACKCD :LEGAL_CATCD :SUBP :DISCCD :DISCDT)}
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
    :create "create table AMPP__REIMBURSEMENT_INFO (APPID integer, PX_CHRGS integer, DISP_FEES integer, BB integer, LTD_STAB integer, CAL_PACK integer,
             SPEC_CONTCD integer, DND integer, FP34D integer,
             foreign key(APPID) REFERENCES AMPP(APPID), foreign key(SPEC_CONTCD) REFERENCES SPEC_CONT(CD))"
    :insert "insert into AMPP__REIMBURSEMENT_INFO (APPID, PX_CHRGS, DISP_FEES, BB, LTD_STAB, CAL_PACK, SPEC_CONTCD, DND, FP34D) VALUES (?,?,?,?,?,?,?,?,?)"
    :data   (juxt :APPID :PX_CHRGS :DISP_FEES :BB :LTD_STAB :CAL_PACK :SPEC_CONTCD :DND :FP34D)}
   {:id     [:AMPP :COMB_CONTENT]
    :create "create table AMPP__COMB_CONTENT (PRNTAPPID integer, CHLDAPPID integer,
             foreign key(PRNTAPPID) REFERENCES AMPP(APPID), foreign key(CHLDAPPID) REFERENCES AMPP(APPID))"
    :insert "insert into AMPP__COMB_CONTENT (PRNTAPPID, CHLDAPPID) VALUES (?,?)"
    :data   (juxt :PRNTAPPID :CHLDAPPID)}
   {:id     [:GTIN :AMPP]
    :create "create table GTIN__AMPP (AMPPID integer, GTIN text, STARTDT text, ENDDT text)"
    :index  "create index GTIN__AMPP_AMPPID_IDX on GTIN__AMPP(AMPPID)"
    :insert "insert into GTIN__AMPP (AMPPID, GTIN, STARTDT, ENDDT) VALUES (?,?,?,?)"
    :data   (juxt :AMPPID :GTIN :STARTDT :ENDDT)}
   {:id     [:BNF :VMPS]
    :create "create table BNF_DETAILS (VPID integer, BNF text, ATC text, DDD text, DDD_UOMCD integer,
             foreign key(DDD_UOMCD) references UNIT_OF_MEASURE(CD))"
    :index  ["create index BNF_DETAILS_VPID_IDX on BNF_DETAILS(VPID)"
             "create index BNF_DETAILS_ATC_IDX on BNF_DETAILS(ATC)"]
    :insert "insert into BNF_DETAILS (VPID, BNF, ATC, DDD, DDD_UOMCD) VALUES (?,?,?,?,?)"
    :data   (juxt :VPID :BNF :ATC :DDD :DDD_UOMCD)}
   {:id     [:HISTORY :HISTORY]
    ;; no foreign keys: most previous identifiers deliberately do not exist in current tables
    :create "create table HISTORY (CLS text, IDCURRENT integer, IDPREVIOUS integer, STARTDT text, ENDDT text)"
    :index  ["create index HISTORY_IDCURRENT_IDX on HISTORY(IDCURRENT)"
             "create index HISTORY_IDPREVIOUS_IDX on HISTORY(IDPREVIOUS)"]
    :insert "insert into HISTORY (CLS, IDCURRENT, IDPREVIOUS, STARTDT, ENDDT) VALUES (?,?,?,?,?)"
    :data   (juxt :CLS :IDCURRENT :IDPREVIOUS :STARTDT :ENDDT)}
   {:id     [:VTM_ING :VTM_ING]
    :create "create table VTM__INGREDIENT (VTMID integer, ISID integer,
             foreign key(VTMID) references VTM(VTMID), foreign key(ISID) references INGREDIENT(ISID))"
    :index  ["create index VTM__INGREDIENT_VTMID_IDX on VTM__INGREDIENT(VTMID)"
             "create index VTM__INGREDIENT_ISID_IDX on VTM__INGREDIENT(ISID)"]
    :insert "insert into VTM__INGREDIENT (VTMID, ISID) VALUES (?,?)"
    :data   (juxt :VTMID :ISID)}])

(when-not
 (s/valid? (s/coll-of ::entity) entities)
  (throw (ex-info "Invalid entity definition" (s/explain-data (s/coll-of ::entity) entities))))

(def entity-by-type
  (reduce (fn [acc {:keys [id] :as entity}] (assoc acc id entity)) {} entities))

(def lookup-types
  "Set of lookup types, as keywords, each of which is the name of a lookup
  table, e.g. #{:BASIS_OF_NAME :SUPPLIER ...}."
  (into #{} (comp (filter #(and (vector? %) (= :LOOKUP (first %)))) (map second))
        (map :id entities)))

(def product-tables
  "Map of product type to its table and primary key column."
  {:VTM  {:table "VTM" :pk "VTMID"}
   :VMP  {:table "VMP" :pk "VPID"}
   :AMP  {:table "AMP" :pk "APID"}
   :VMPP {:table "VMPP" :pk "VPPID"}
   :AMPP {:table "AMPP" :pk "APPID"}})

(s/fdef create-tables
  :args (s/cat :conn ::conn))

(defn create-tables
  "Creates all database tables for dm+d entities."
  [conn]
  (run! #(jdbc/execute-one! conn [%]) (map :create entities)))

(defn create-search-index
  "Creates and populates a full-text (FTS5) index of product names. This is
  derived data, built after import rather than being an import target."
  [conn]
  (jdbc/execute-one! conn ["create virtual table SEARCH using fts5(ID unindexed, TYPE unindexed, NM)"])
  (doseq [[product-type {:keys [table pk]}] product-tables]
    (jdbc/execute-one! conn [(str "insert into SEARCH (ID, TYPE, NM) select " pk ", '" (name product-type) "', NM from " table)])))

(s/fdef create-indexes
  :args (s/cat :conn ::conn))
(defn create-indexes
  "Creates all database indexes; each entity may define none, one, or many."
  [conn]
  (->> entities
       (mapcat (fn [{:keys [index]}] (if (string? index) [index] index)))
       (run! #(jdbc/execute! conn [%]))))

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

;; First 16 bytes of every SQLite 3 file: the literal "SQLite format 3\000".
(def ^:private ^"[B" sqlite-magic
  (byte-array (map byte (concat "SQLite format 3" [0]))))

(defn- read-sqlite-header
  "Read the 100-byte SQLite header. Returns the byte array, or nil if the
  file does not exist, is not a regular file, or is shorter than 100 bytes."
  ^"[B" [f]
  (let [file (io/file f)]
    (when (and (.isFile file) (>= (.length file) 100))
      (with-open [in (io/input-stream file)]
        (let [buf (byte-array 100)]
          (when (= 100 (.readNBytes in buf 0 100)) buf))))))

(defn- header-int32
  "Read a big-endian unsigned 32-bit int at offset `off`."
  ^long [^bytes header ^long off]
  (bit-or (bit-shift-left (bit-and 0xFF (aget header off)) 24)
          (bit-shift-left (bit-and 0xFF (aget header (+ off 1))) 16)
          (bit-shift-left (bit-and 0xFF (aget header (+ off 2))) 8)
          (bit-and 0xFF (aget header (+ off 3)))))

(defn- sqlite-magic? [^bytes header]
  (java.util.Arrays/equals (java.util.Arrays/copyOfRange header 0 16) sqlite-magic))

(defn sqlite-database?
  "Returns true if `f` is a SQLite 3 database file (checked by header magic)."
  [f]
  (boolean (some-> (read-sqlite-header f) sqlite-magic?)))

(defn dmd-database?
  "Returns true if `f` is a dm+d SQLite database created by this library
  (SQLite magic + application_id == 0x646D2B64). Strict by default: legacy
  dm+d databases predating application_id support return false."
  [f]
  (boolean (when-let [header (read-sqlite-header f)]
             (and (sqlite-magic? header)
                  (= application-id (header-int32 header 68))))))

(defn- pool
  ^HikariDataSource [filename {:keys [maximum-pool-size init-sql]}]
  (connection/->pool HikariDataSource
                     (cond-> {:jdbcUrl         (str "jdbc:sqlite:" filename)
                              :maximumPoolSize (or maximum-pool-size 4)}
                       init-sql (assoc :connectionInitSql init-sql))))

(defn- read-pragma [conn pragma]
  (some-> (jdbc/execute-one! conn [(str "PRAGMA " pragma)]) vals first))

(defn open-store
  "Open a read-only dm+d store, returning a pooled DataSource that supports
  concurrent use. Throws an exception if the file does not exist, is not a
  dm+d database (checked via SQLite application_id), or was created with an
  incompatible store version (checked via SQLite user_version). Legacy
  databases, including those predating application_id support, are rejected
  and must be rebuilt using this version of the library."
  [filename]
  (when-not (.exists (io/file filename))
    (throw (ex-info (str "file not found: " filename) {:filename filename})))
  (when-not (dmd-database? filename)
    (throw (ex-info (str (if (sqlite-database? filename)
                           "not a dm+d database (or a legacy dm+d database that must be rebuilt): "
                           "not a SQLite database: ") filename)
                    {:filename filename})))
  (let [ds      (pool filename {:init-sql "PRAGMA query_only = 1"})
        version (read-pragma ds "user_version")]
    (when-not (= store-version version)
      (.close ds)
      (throw (ex-info (str "incompatible dm+d database version: found " version
                           ", expected " store-version "; rebuild the database using this version of dmd")
                      {:filename filename :version version :expected store-version})))
    ds))

(defn close
  "Close the store, releasing pooled connections."
  [^HikariDataSource ds]
  (when ds (.close ds)))

(s/fdef fetch-release-date
  :args (s/cat :conn ::conn))
(defn fetch-release-date
  [conn]
  (some-> (:METADATA/release (jdbc/execute-one! conn ["select release from metadata"])) LocalDate/parse))

(def ^:private count-tables
  "Tables counted by [[status]], as pairs of key and table name."
  [[:VTM "VTM"] [:VMP "VMP"] [:AMP "AMP"] [:VMPP "VMPP"] [:AMPP "AMPP"]
   [:INGREDIENT "INGREDIENT"] [:HISTORY "HISTORY"] [:VTM_INGREDIENT "VTM__INGREDIENT"]
   [:GTIN "GTIN__AMPP"] [:BNF "BNF_DETAILS"]])

(s/fdef status
  :args (s/cat :conn ::conn))
(defn status
  "Returns a structured description of an open dm+d store:
  - :version - store (schema) version
  - :created - java.time.LocalDateTime the database was created
  - :release - java.time.LocalDate of the dm+d release
  - :trud    - source TRUD release information, when installed via TRUD
  - :files   - inventory of imported source files
  - :counts  - a map of entity type to row count"
  [conn]
  (let [{:METADATA/keys [version created release trud]} (jdbc/execute-one! conn ["select * from metadata"])]
    {:version version
     :created (some-> created LocalDateTime/parse)
     :release (some-> release LocalDate/parse)
     :trud    (some-> trud (json/read-str :key-fn keyword))
     :files   (mapv (fn [{:FILES/keys [type name date]}]
                      {:type (keyword type) :name name :date (some-> date LocalDate/parse)})
                    (jdbc/execute! conn ["select * from files"]))
     :counts  (reduce (fn [acc [k table]]
                        (assoc acc k (:n (jdbc/execute-one! conn [(str "select count(*) as n from " table)]))))
                      {} count-tables)}))

(defn create-store
  "Create a dm+d store at `filename` from the directories `dirs`.
  Options:
  - :batch-size   - number of components per write batch (default 50000)
  - :release-date - date of the dm+d release
  - :trud         - source TRUD release information, stored for provenance"
  [filename dirs & {:keys [batch-size release-date trud] :or {batch-size 50000}}]
  (when (.exists (io/file filename))
    (throw (ex-info (str "dm+d database already exists: " filename) {})))
  (let [ds (pool filename {:maximum-pool-size 1})    ;; SQLite serialises writers; one connection is enough
        ch (async/chan 5 (comp (partition-all batch-size) (map batch->sql)))]
    (try
      (async/thread
        (doseq [dir dirs]
          (dim/stream-dmd dir ch :close? false))
        (async/close! ch))
      (jdbc/execute! ds [(str "PRAGMA application_id = " application-id)])
      (jdbc/execute! ds [(str "PRAGMA user_version = " store-version)])
      (create-tables ds)
      (jdbc/execute! ds ["insert into metadata (version, created, release, trud) values (?,?,?,?)"
                         store-version (LocalDateTime/now) release-date (some-> trud json/write-str)])
      (jdbc/execute-batch! ds "insert into FILES (type, name, date) values (?,?,?)"
                           (mapv (fn [{:keys [type date ^java.io.File file]}]
                                   [(name type) (.getName file) (str date)])
                                 (mapcat dim/dmd-file-seq dirs))
                           {})
      (loop [{:keys [stmts errors] :as batch} (async/<!! ch), all-errors #{}]
        (if-not batch
          (do (create-indexes ds)
              (create-search-index ds)
              {:errors (seq all-errors)})
          (do
            (jdbc/with-transaction [txn ds]
              (doseq [{:keys [stmt data]} stmts]
                (jdbc/execute-batch! txn stmt data {})))
            (recur (async/<!! ch), (into all-errors errors)))))
      (catch Exception e
        (log/warn (ex-message e))
        {:errors e})
      (finally
        (.close ds)))))

;;;
;;;
;;;
(s/fdef fetch-lookup
  :args (s/cat :conn ::conn :lookup ::lookup :code ::code))
(defn fetch-lookup [conn lookup code]
  (when code (jdbc/execute-one! conn [(str "select * from " (name lookup) " WHERE CD=?") code])))

(s/fdef fetch-all-lookup
  :args (s/cat :conn ::conn :lookup ::lookup))
(defn fetch-all-lookup [conn lookup]
  (jdbc/execute! conn [(str "select * from " (name lookup))]))

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
  "Returns the ingredient with the given ISID."
  [conn isid]
  (jdbc/execute-one! conn ["select * from ingredient where isid=?" isid]))

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

(s/fdef fetch-vmpp-drug-tariff-info
  :args (s/cat :conn ::conn :vppid ::vppid))
(defn ^:private fetch-vmpp-drug-tariff-info
  [conn vppid]
  (when-let [{:VMPP__DRUG_TARIFF_INFO/keys [PAY_CATCD] :as dti}
             (jdbc/execute-one! conn ["select * from VMPP__DRUG_TARIFF_INFO where vppid=?" vppid])]
    (assoc dti :VMPP__DRUG_TARIFF_INFO/PAY_CAT (fetch-lookup conn :DT_PAYMENT_CATEGORY PAY_CATCD))))

(s/fdef fetch-vmpp-comb-content
  :args (s/cat :conn ::conn :vppid ::vppid))
(defn ^:private fetch-vmpp-comb-content
  "Returns the child packs of the given VMPP, when it is a combination pack."
  [conn vppid]
  (->> (jdbc/execute! conn ["select * from VMPP__COMB_CONTENT where PRNTVPPID=?" vppid])
       (map (fn [{:VMPP__COMB_CONTENT/keys [CHLDVPPID] :as cc}]
              (assoc cc :VMPP__COMB_CONTENT/CHLD (fetch-vmpp* conn CHLDVPPID))))))

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
           :VMPP/COMBPACK (fetch-lookup conn :COMBINATION_PACK_IND COMBPACKCD)
           :VMPP/DRUG_TARIFF_INFO (fetch-vmpp-drug-tariff-info conn vppid) ;; to-one
           :VMPP/COMB_CONTENT (fetch-vmpp-comb-content conn vppid))))      ;; to-many

(s/fdef fetch-amp*
  :args (s/cat :conn ::conn :apid ::apid))
(defn fetch-amp*
  "Return the given AMP without extended information."
  [conn apid]
  (some-> (jdbc/execute-one! conn ["select * from amp where apid=?" apid])
          (assoc :TYPE "AMP")))

(s/fdef fetch-amp-excipients
  :args (s/cat :conn ::conn :apid ::apid))
(defn ^:private fetch-amp-excipients
  "Returns excipients (AP_INGREDIENT) for the given AMP."
  [conn apid]
  (->> (jdbc/execute! conn ["select * from AMP__AP_INGREDIENT where apid=?" apid])
       (map (fn [{:AMP__AP_INGREDIENT/keys [ISID UOMCD] :as api}]
              (assoc api :AMP__AP_INGREDIENT/IS (fetch-ingredient conn ISID)
                     :AMP__AP_INGREDIENT/UOM (fetch-lookup conn :UNIT_OF_MEASURE UOMCD))))))

(s/fdef fetch-amp-licensed-routes
  :args (s/cat :conn ::conn :apid ::apid))
(defn ^:private fetch-amp-licensed-routes
  [conn apid]
  (->> (jdbc/execute! conn ["select * from AMP__LICENSED_ROUTE where apid=?" apid])
       (map (fn [{:AMP__LICENSED_ROUTE/keys [ROUTECD] :as lr}]
              (assoc lr :AMP__LICENSED_ROUTE/ROUTE (fetch-lookup conn :ROUTE ROUTECD))))))

(s/fdef fetch-amp-information
  :args (s/cat :conn ::conn :apid ::apid))
(defn ^:private fetch-amp-information
  [conn apid]
  (when-let [{:AMP__AP_INFORMATION/keys [COLOURCD] :as info}
             (jdbc/execute-one! conn ["select * from AMP__AP_INFORMATION where apid=?" apid])]
    (assoc info :AMP__AP_INFORMATION/COLOUR (fetch-lookup conn :COLOUR COLOURCD))))

(s/fdef fetch-amp
  :args (s/cat :conn ::conn :apid ::apid))
(defn fetch-amp
  "Return the given AMP with extended information, including the parent VMP."
  [conn apid]
  (when-let [{:AMP/keys [VPID SUPPCD LIC_AUTHCD AVAIL_RESTRICTCD FLAVOURCD COMBPRODCD] :as amp}
             (jdbc/execute-one! conn ["select * from amp where apid=?" apid])]
    (assoc amp
           :TYPE "AMP"
           :AMP/SUPP (fetch-lookup conn :SUPPLIER SUPPCD)
           :AMP/LIC_AUTH (fetch-lookup conn :LICENSING_AUTHORITY LIC_AUTHCD)
           :AMP/AVAIL_RESTRICT (fetch-lookup conn :AVAILABILITY_RESTRICTION AVAIL_RESTRICTCD)
           :AMP/FLAVOUR (fetch-lookup conn :FLAVOUR FLAVOURCD)
           :AMP/COMBPROD (fetch-lookup conn :COMBINATION_PROD_IND COMBPRODCD)
           :AMP/VP (fetch-vmp conn VPID)
           :AMP/AP_INGREDIENTS (fetch-amp-excipients conn apid)      ;; to-many: excipients
           :AMP/LICENSED_ROUTES (fetch-amp-licensed-routes conn apid) ;; to-many
           :AMP/AP_INFORMATION (fetch-amp-information conn apid))))   ;; to-one

(s/fdef fetch-ampp*
  :args (s/cat :conn ::conn :appid ::appid))
(defn fetch-ampp*
  "Return the given AMPP without extended information."
  [conn appid]
  (some-> (jdbc/execute-one! conn ["select * from ampp where appid=?" appid])
          (assoc :TYPE "AMPP")))

(s/fdef fetch-ampp-appliance-pack-info
  :args (s/cat :conn ::conn :appid ::appid))
(defn ^:private fetch-ampp-appliance-pack-info
  [conn appid]
  (when-let [{:AMPP__APPLIANCE_PACK_INFO/keys [REIMB_STATCD] :as info}
             (jdbc/execute-one! conn ["select * from AMPP__APPLIANCE_PACK_INFO where appid=?" appid])]
    (assoc info :AMPP__APPLIANCE_PACK_INFO/REIMB_STAT (fetch-lookup conn :REIMBURSEMENT_STATUS REIMB_STATCD))))

(s/fdef fetch-ampp-prescribing-info
  :args (s/cat :conn ::conn :appid ::appid))
(defn ^:private fetch-ampp-prescribing-info
  [conn appid]
  (jdbc/execute-one! conn ["select * from AMPP__DRUG_PRODUCT_PRESCRIB_INFO where appid=?" appid]))

(s/fdef fetch-ampp-price-info
  :args (s/cat :conn ::conn :appid ::appid))
(defn ^:private fetch-ampp-price-info
  [conn appid]
  (when-let [{:AMPP__MEDICINAL_PRODUCT_PRICE/keys [PRICE_BASISCD] :as price}
             (jdbc/execute-one! conn ["select * from AMPP__MEDICINAL_PRODUCT_PRICE where appid=?" appid])]
    (assoc price :AMPP__MEDICINAL_PRODUCT_PRICE/PRICE_BASIS (fetch-lookup conn :PRICE_BASIS PRICE_BASISCD))))

(s/fdef fetch-ampp-reimbursement-info
  :args (s/cat :conn ::conn :appid ::appid))
(defn ^:private fetch-ampp-reimbursement-info
  [conn appid]
  (when-let [{:AMPP__REIMBURSEMENT_INFO/keys [SPEC_CONTCD DND] :as ri}
             (jdbc/execute-one! conn ["select * from AMPP__REIMBURSEMENT_INFO where appid=?" appid])]
    (assoc ri :AMPP__REIMBURSEMENT_INFO/SPEC_CONT (fetch-lookup conn :SPEC_CONT SPEC_CONTCD)
           :AMPP__REIMBURSEMENT_INFO/DND_IND (fetch-lookup conn :DND DND))))

(s/fdef fetch-ampp-comb-content
  :args (s/cat :conn ::conn :appid ::appid))
(defn ^:private fetch-ampp-comb-content
  "Returns the child packs of the given AMPP, when it is a combination pack."
  [conn appid]
  (->> (jdbc/execute! conn ["select * from AMPP__COMB_CONTENT where PRNTAPPID=?" appid])
       (map (fn [{:AMPP__COMB_CONTENT/keys [CHLDAPPID] :as cc}]
              (assoc cc :AMPP__COMB_CONTENT/CHLD (fetch-ampp* conn CHLDAPPID))))))

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
           :AMPP/DISC (fetch-lookup conn :DISCONTINUED_IND DISCCD)
           :AMPP/APPLIANCE_PACK_INFO (fetch-ampp-appliance-pack-info conn appid)        ;; to-one
           :AMPP/DRUG_PRODUCT_PRESCRIB_INFO (fetch-ampp-prescribing-info conn appid)    ;; to-one
           :AMPP/MEDICINAL_PRODUCT_PRICE (fetch-ampp-price-info conn appid)             ;; to-one
           :AMPP/REIMBURSEMENT_INFO (fetch-ampp-reimbursement-info conn appid)          ;; to-one
           :AMPP/COMB_CONTENT (fetch-ampp-comb-content conn appid))))                   ;; to-many

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

(defn ^:private fts5-query
  "Turn a user-entered string into a safe FTS5 MATCH expression: each
  whitespace-delimited token is quoted (preventing FTS5 query syntax
  injection) and matched as a prefix; multiple tokens combine as AND."
  [s]
  (->> (str/split (or s "") #"\s+")
       (remove str/blank?)
       (map #(str "\"" (str/replace % "\"" "\"\"") "\"*"))
       (str/join " ")))

(s/def ::types (s/coll-of (set (keys product-tables))))
(s/def ::limit pos-int?)
(s/fdef search
  :args (s/cat :conn ::conn :s (s/nilable string?)
               :opts (s/keys* :opt-un [::types ::limit])))
(defn search
  "Search product names, returning a sequence of maps of :SEARCH/ID,
  :SEARCH/TYPE and :SEARCH/NM, best matches first. Each token of `s` is
  matched as a prefix; multiple tokens must all match.
  Options:
  - :types - product types to include e.g. #{:VMP :AMP}; default, all
  - :limit - maximum number of results; default 100"
  [conn s & {:keys [types limit] :or {limit 100}}]
  (let [q (fts5-query s)]
    (if (str/blank? q)
      []
      (if (seq types)
        (jdbc/execute! conn (into [(str "select ID, TYPE, NM from SEARCH where NM match ? and TYPE in ("
                                        (str/join "," (repeat (count types) "?"))
                                        ") order by rank limit ?") q]
                                  (concat (map name types) [limit])))
        (jdbc/execute! conn ["select ID, TYPE, NM from SEARCH where NM match ? order by rank limit ?" q limit])))))

(s/fdef fetch-history
  :args (s/cat :conn ::conn :id ::product-id))
(defn fetch-history
  "Returns all history entries in which `id` is the current identifier,
  ordered by start date. Includes 'self' entries (IDCURRENT=IDPREVIOUS) which
  record the period of validity of the current identifier itself."
  [conn id]
  (jdbc/execute! conn ["select * from HISTORY where IDCURRENT=? order by STARTDT" id]))

(s/fdef previous-ids
  :args (s/cat :conn ::conn :id ::product-id))
(defn previous-ids
  "Returns the set of prior identifiers for the given current identifier,
  excluding the identifier itself."
  [conn id]
  (into #{} (map :IDPREVIOUS)
        (jdbc/plan conn ["select distinct IDPREVIOUS from HISTORY where IDCURRENT=? and IDPREVIOUS<>IDCURRENT" id])))

(s/fdef current-ids
  :args (s/cat :conn ::conn :id ::product-id))
(defn current-ids
  "Returns the set of identifiers in current use for the given (usually
  historic) identifier, excluding the identifier itself. Usually a single
  identifier, but the data model permits more than one."
  [conn id]
  (into #{} (map :IDCURRENT)
        (jdbc/plan conn ["select distinct IDCURRENT from HISTORY where IDPREVIOUS=? and IDPREVIOUS<>IDCURRENT" id])))

(s/fdef isids-for-vtm
  :args (s/cat :conn ::conn :vtmid ::vtmid))
(defn isids-for-vtm
  "Returns ingredient (ISID) identifiers for the given VTM."
  [conn vtmid]
  (into [] (map :ISID) (jdbc/plan conn ["select ISID from VTM__INGREDIENT where VTMID=?" vtmid])))

(s/fdef vtmids-for-ingredient
  :args (s/cat :conn ::conn :isid ::isid))
(defn vtmids-for-ingredient
  "Returns VTM identifiers for the given ingredient."
  [conn isid]
  (into [] (map :VTMID) (jdbc/plan conn ["select VTMID from VTM__INGREDIENT where ISID=?" isid])))

(s/def ::gtin (s/or :str string? :num int?))
(s/def ::on-date #(instance? LocalDate %))
(s/def ::include-expired boolean?)

;; GTIN assignments carry a period of validity: a GTIN applies from its start
;; date to its end date inclusive; an entry with no end date remains current.
(def ^:private gtin-validity-clause
  "(STARTDT is null or STARTDT<=?) and (ENDDT is null or ENDDT>=?)")

(s/fdef gtins-for-appid
  :args (s/cat :conn ::conn :appid ::appid
               :opts (s/keys* :opt-un [::on-date ::include-expired])))
(defn gtins-for-appid
  "Returns GTINs (Global Trade Item Numbers) for the given AMPP, as strings.
  By default, only GTINs valid on the current date are returned:
  a GTIN is valid from its start date to its end date inclusive, so expired
  entries, such as for packs no longer in circulation, are omitted.
  Options:
  - :on-date         - java.time.LocalDate on which to assess validity;
                       default, today
  - :include-expired - when true, return all GTINs irrespective of their
                       period of validity"
  [conn appid & {:keys [on-date include-expired]}]
  (if include-expired
    (into [] (map :GTIN)
          (jdbc/plan conn ["select GTIN from GTIN__AMPP where AMPPID=?" appid]))
    (let [d (str (or on-date (LocalDate/now)))]
      (into [] (map :GTIN)
            (jdbc/plan conn [(str "select GTIN from GTIN__AMPP where AMPPID=? and " gtin-validity-clause) appid d d])))))

(s/fdef appids-from-gtin
  :args (s/cat :conn ::conn :gtin ::gtin
               :opts (s/keys* :opt-un [::on-date ::include-expired])))
(defn appids-from-gtin
  "Returns AMPP identifiers for the given GTIN (Global Trade Item Number),
  which may be a string or a number. Usually a single identifier, but the
  data model permits more than one. By default, only assignments valid on
  the current date are returned: a GTIN is valid from its start date to its
  end date inclusive, so expired entries are omitted.
  Options:
  - :on-date         - java.time.LocalDate on which to assess validity;
                       default, today
  - :include-expired - when true, return all AMPPs irrespective of their
                       period of validity"
  [conn gtin & {:keys [on-date include-expired]}]
  (if include-expired
    (into [] (map :AMPPID)
          (jdbc/plan conn ["select AMPPID from GTIN__AMPP where GTIN=?" (str gtin)]))
    (let [d (str (or on-date (LocalDate/now)))]
      (into [] (map :AMPPID)
            (jdbc/plan conn [(str "select AMPPID from GTIN__AMPP where GTIN=? and " gtin-validity-clause) (str gtin) d d])))))

(s/fdef plan-products
  :args (s/cat :conn ::conn :product-type (set (keys product-tables))))
(defn plan-products
  "Returns a reducible (clojure.lang.IReduceInit) over all rows of the given
  product type (:VTM :VMP :AMP :VMPP or :AMPP), for streaming iteration
  without realising all rows in memory. Each row is a `next.jdbc` row
  abstraction; access columns by keyword, e.g. (map :NM)."
  [conn product-type]
  (let [{:keys [table]} (product-tables product-type)]
    (jdbc/plan conn [(str "select * from " table)])))

(s/fdef plan-ingredients
  :args (s/cat :conn ::conn))
(defn plan-ingredients
  "Returns a reducible (clojure.lang.IReduceInit) over all rows of the
  INGREDIENT table, for streaming iteration without realising all rows in
  memory. Each row is a `next.jdbc` row abstraction; access columns by
  keyword, e.g. (map :ISID) or (map :NM)."
  [conn]
  (jdbc/plan conn ["select * from INGREDIENT"]))

(defn ^:private atc->like [s]
  (-> s (str/replace "*" "%") (str/replace "?" "_")))

(s/fdef vpids-from-atc
  :args (s/cat :conn ::conn :atc ::atc))
(defn vpids-from-atc
  "Return the VPIDs matching the given ATC code."
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
  "Return the VPIDs matching the given ATC code/prefix that do not
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

(s/fdef apids-for-vpids
  :args (s/cat :conn ::conn :vpids (s/coll-of ::vpid)))
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
  :args (s/cat :conn ::conn :vppids (s/coll-of ::vppid)))
(defn appids-for-vppids
  [conn vppids]
  (into [] (map :APPID)
        (jdbc/plan conn (sql/format {:select :appid :from :ampp :where [:in :vppid vppids]}))))

(s/fdef vppids-for-appids
  :args (s/cat :conn ::conn :appids (s/coll-of ::appid)))
(defn vppids-for-appids
  [conn appids]
  (into [] (map :VPPID)
        (jdbc/plan conn (sql/format {:select :vppid :from :ampp :where [:in :appid appids]}))))

(s/fdef parent-vppids-for-vppid
  :args (s/cat :conn ::conn :vppid ::vppid))
(defn parent-vppids-for-vppid
  "Return the VPPIDs of the combination pack(s) of which the given VMPP is a
  child component; empty when the VMPP is not a child of any combination pack.
  This is the reverse of [[fetch-vmpp-comb-content]]."
  [conn vppid]
  (into [] (map :PRNTVPPID)
        (jdbc/plan conn (sql/format {:select :prntvppid :from :vmpp__comb_content :where [:= :chldvppid vppid]}))))

(s/fdef parent-appids-for-appid
  :args (s/cat :conn ::conn :appid ::appid))
(defn parent-appids-for-appid
  "Return the APPIDs of the combination pack(s) of which the given AMPP is a
  child component; empty when the AMPP is not a child of any combination pack.
  This is the reverse of [[fetch-ampp-comb-content]]."
  [conn appid]
  (into [] (map :PRNTAPPID)
        (jdbc/plan conn (sql/format {:select :prntappid :from :ampp__comb_content :where [:= :chldappid appid]}))))

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
  "Return the set of VTM ids for the given product."
  [conn id]
  (case (product-type conn id)
    :VTM #{id}
    :VMP (vtmids-for-vpids conn [id])
    :VMPP (vtmids-for-vpids conn (vpids-for-vmpps conn [id]))
    :AMP (vtmids-for-vpids conn (vpids-for-apids conn [id]))
    :AMPP (vtmids-for-vpids conn (vpids-for-apids conn (apids-for-appids conn [id])))
    nil))

(s/fdef apids
  :args (s/cat :conn ::conn :id ::product-id))
(defn apids
  "Return AMP ids for the given product."
  [conn id]
  (case (product-type conn id)
    :VTM (apids-for-vpids conn (vpids-for-vtmids conn [id]))
    :VMP (apids-for-vpids conn [id])
    :VMPP (apids-for-vpids conn (vpids-for-vmpps conn [id]))
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
  :args (s/cat :conn ::conn :id ::product-id))
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

(s/fdef subsumes?
  :args (s/cat :conn ::conn :a ::product-id :b ::product-id))
(defn subsumes?
  "Returns true if product `a` subsumes product `b`: that is, `a` is an
  ancestor of `b` in the dm+d product hierarchy, in which an AMPP is both an
  AMP and a VMPP, an AMP and a VMPP are each a VMP, and a VMP is a VTM.
  Strict: a product does not subsume itself, so equal identifiers return
  false, as does any identifier that is not a current product identifier;
  equivalence is left to the caller."
  [conn a b]
  (boolean
   (when-not (= a b)
     (case (product-type conn b)
       :VMP  (some #(= a %) (vtmids conn b))
       :AMP  (or (some #(= a %) (vpids conn b))
                 (some #(= a %) (vtmids conn b)))
       :VMPP (or (some #(= a %) (vpids conn b))
                 (some #(= a %) (vtmids conn b)))
       :AMPP (or (some #(= a %) (apids-for-appids conn [b]))
                 (some #(= a %) (vppids-for-appids conn [b]))
                 (some #(= a %) (vpids conn b))
                 (some #(= a %) (vtmids conn b)))
       nil))))

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
