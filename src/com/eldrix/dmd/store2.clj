(ns com.eldrix.dmd.store2
  "A datalog store for dm+d."
  (:require [clojure.core.async :as a]
            [clojure.core.match :refer [match]]
            [clojure.tools.logging.readable :as log]
            [datalevin.core :as d]
            [com.eldrix.dmd.import :as dim]
            [clojure.set :as set]
            [clojure.string :as str])
  (:import (java.io Closeable)
           (java.util.regex Pattern)))

(deftype DmdStore [conn]
  Closeable
  (close [_] (d/close conn)))

(def schema
  "The datalog-based schema is a close representation to the source dm+d data
  structures. The key characteristics are:
   - all products are given a :PRODUCT/ID property and a :PRODUCT/TYPE property.
   - properties are namespaced using a code representing the filename from which
   they were derived (e.g. :VTM :VMP :AMP etc), except lookups are given their
   own namespace representing that lookup (e.g. :BASIS_OF_NAME).
   - lookups are referenced by their code (e.g. :BASIS_OF_NAME/CD 3) and
   the source entity has a property :VMP/BASIS based on the original reference
   (e.g. :VMP/BASISCD)."
  {:PRODUCT/ID                           {:db/unique    :db.unique/identity
                                          :db/valueType :db.type/long}
   :PRODUCT/TYPE                         {:db/valueType :db.type/keyword}

   ;; VTM
   :VTM/VTMID                            {:db/valueType :db.type/long}
   :VTM/INVALID                          {:db/valueType :db.type/boolean}
   :VTM/VTMIDPREV                        {:db/valueType :db.type/long}

   ;; VMP
   :VMP/VPID                             {:db/valueType :db.type/long}
   :VMP/VPIDPREV                         {:db/valueType :db.type/long}
   :VMP/VTMID                            {:db/valueType :db.type/long}
   :VMP/VTM                              {:db/valueType :db.type/ref}
   :VMP/INVALID                          {:db/valueType :db.type/boolean}
   :VMP/NM                               {:db/valueType :db.type/string}
   :VMP/ABBREVNM                         {:db/valueType :db.type/string}
   :VMP/BASIS                            {:db/valueType :db.type/ref}
   :VMP/NMPREV                           {:db/valueType :db.type/string}
   :VMP/BASIS_PREV                       {:db/valueType :db.type/ref}
   :VMP/NMCHANGE                         {:db/valueType :db.type/ref}
   :VMP/COMBPROD                         {:db/valueType :db.type/ref}
   :VMP/PRES_STAT                        {:db/valueType :db.type/ref}
   :VMP/SUG_F                            {:db/valueType :db.type/boolean}
   :VMP/GLU_F                            `{:db/valueType :db.type/boolean}
   :VMP/PRES_F                           {:db/valueType :db.type/boolean}
   :VMP/CFC_F                            {:db/valueType :db.type/boolean}
   :VMP/NON_AVAIL                        {:db/valueType :db.type/ref}
   :VMP/DF_IND                           {:db/valueType :db.type/ref}
   :VMP/UDFS                             {:db/valueType :db.type/double}
   :VMP/UDFS_UOMCD                       {:db/valueType :db.type/long}
   :VMP/UDFS_UOM                         {:db/valueType :db.type/ref}
   :VMP/UNIT_DOSE_UOMCD                  {:db/valueType :db.type/long}
   :VMP/UNIT_DOSE_UOM                    {:db/valueType :db.type/ref}
   :VMP/INGREDIENTS                      {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :VMP/ONT_DRUG_FORMS                   {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :VMP/DRUG_FORM                        {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :VMP/DRUG_ROUTES                      {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :VMP/CONTROL_DRUG_INFO                {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :VMP/BNF_DETAILS                      {:db/valueType :db.type/ref}

   ;; VMP - VPIs - ingredients
   :VPI/PRODUCT                          {:db/valueType :db.type/ref}
   :VPI/ISID                             {:db/valueType :db.type/long}
   :VPI/IS                               {:db/valueType :db.type/ref}
   :VPI/BASIS_STRNT                      {:db/valueType :db.type/ref}
   :VPI/BS_SUB                           {:db/valueType :db.type/ref}
   :VPI/STRNT_NMRTR_VAL                  {:db/valueType :db.type/double}
   :VPI/STRNT_NMRTR_UOMCD                {:db/valueType :db.type/long}
   :VPI/STRNT_NMRTR_UOM                  {:db/valueType :db.type/ref}

   ;; AMP
   :AMP/APID                             {:db/valueType :db.type/long}
   :AMP/INVALID                          {:db/valueType :db.type/boolean}
   :AMP/VPID                             {:db/valueType :db.type/long}
   :AMP/VP                               {:db/valueType :db.type/ref}
   :AMP/NM                               {:db/valueType :db.type/string}
   :AMP/ABBREVNM                         {:db/valueType :db.type/string}
   :AMP/DESC                             {:db/valueType :db.type/string}
   :AMP/NM_PREV                          {:db/valueType :db.type/string}
   :AMP/SUPPCD                           {:db/valueType :db.type/long}
   :AMP/SUPP                             {:db/valueType :db.type/ref}
   :AMP/LIC_AUTHCD                       {:db/valueType :db.type/long}
   :AMP/LIC_AUTH                         {:db/valueType :db.type/ref}
   :AMP/LIC_AUTH_PREVCD                  {:db/valueType :db.type/long}
   :AMP/LIC_AUTH_PREV                    {:db/valueType :db.type/ref}
   :AMP/LIC_AUTHCHANGE                   {:db/valueType :db.type/ref}
   :AMP/COMBPROD                         {:db/valueType :db.type/ref}
   :AMP/FLAVOUR                          {:db/valueType :db.type/ref}
   :AMP/FLAVOURCD                        {:db/valueType :db.type/long}
   :AMP/EMA                              {:db/valueType :db.type/boolean}
   :AMP/PARALLEL_IMPORT                  {:db/valueType :db.type/boolean}
   :AMP/AVAIL_RESTRICT                   {:db/valueType :db.type/ref}
   :AMP/EXCIPIENTS                       {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :AMP/AP_INFORMATION                   {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :AMP/LICENSED_ROUTES                  {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :AMP/COLOUR                           {:db/valueType :db.type/ref}
   :AMP/BNF_DETAILS                      {:db/valueType :db.type/ref}

   ;; AMP - AP-INGREDIENT   (excipients)
   :AP_INGREDIENT/APID                   {:db/valueType :db.type/long}
   :AP_INGREDIENT/ISID                   {:db/valueType :db.type/long}
   :AP_INGREDIENT/IS                     {:db/valueType :db.type/ref}
   :AP_INGREDIENT/STRNTH                 {:db/valueType :db.type/double}
   :AP_INGREDIENT/UOMCD                  {:db/valueType :db.type/long}
   :AP_INGREDIENT/UOM                    {:db/valueType :db.type/ref}

   ;; VMPP
   :VMPP/VPPID                           {:db/valueType :db.type/long}
   :VMPP/INVALID                         {:db/valueType :db.type/boolean}
   :VMPP/NM                              {:db/valueType :db.type/string}
   :VMPP/VPID                            {:db/valueType :db.type/long}
   :VMPP/VP                              {:db/valueType :db.type/ref}
   :VMPP/QTYVAL                          {:db/valueType :db.type/double}
   :VMPP/QTY_UOMCD                       {:db/valueType :db.type/long}
   :VMPP/QTY_UOM                         {:db/valueType :db.type/ref}
   :VMPP/COMBPACKCD                      {:db/valueType :db.type/long}
   :VMPP/COMBPACK                        {:db/valueType :db.type/ref}
   :VMPP/DRUG_TARIFF_INFO                {:db/valueType :db.type/ref}
   :VMPP/CHLDVPP                         {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}

   ;; AMPP
   :AMPP/APPID                           {:db/valueType :db.type/long}
   :AMPP/INVALID                         {:db/valueType :db.type/boolean}
   :AMPP/NM                              {:db/valueType :db.type/string}
   :AMPP/ABBREVNM                        {:db/valueType :db.type/string}
   :AMPP/VPPID                           {:db/valueType :db.type/long}
   :AMPP/VPP                             {:db/valueType :db.type/ref}
   :AMPP/APID                            {:db/valueType :db.type/long}
   :AMPP/AP                              {:db/valueType :db.type/ref}
   :AMPP/COMBPACK                        {:db/valueType :db.type/ref}
   :AMPP/LEGAL_CAT                       {:db/valueType :db.type/ref}
   :AMPP/SUBP                            {:db/valueType :db.type/string}
   :AMPP/DISC                            {:db/valueType :db.type/ref}
   :AMPP/GTIN_DETAILS                    {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :AMPP/APPLIANCE_PACK_INFO             {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :AMPP/REIMBURSEMENT_INFO              {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :AMPP/MEDICINAL_PRODUCT_PRICE         {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :APPLIANCE_PACK_INFO/REIMB_STAT       {:db/valueType :db.type/ref}
   :APPLIANCE_PACK_INFO/REIMB_STATPREV   {:db/valueType :db.type/ref}
   :APPLIANCE_PACK_INFO/PACK_ORDER_NO    {:db/valueType :db.type/string}
   :REIMBURSEMENT_INFO/SPEC_CONT         {:db/valueType :db.type/ref}
   :MEDICINAL_PRODUCT_PRICE/PRICE        {:db/valueType :db.type/double}
   :MEDICINAL_PRODUCT_PRICE/PRICE_BASIS  {:db/valueType :db.type/ref}

   ;; lookups
   :COMBINATION_PACK_IND/CD              {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :COMBINATION_PROD_IND/CD              {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :BASIS_OF_NAME/CD                     {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :NAMECHANGE_REASON/CD                 {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :VIRTUAL_PRODUCT_PRES_STATUS/CD       {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :CONTROL_DRUG_CATEGORY/CD             {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :LICENSING_AUTHORITY/CD               {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :UNIT_OF_MEASURE/CD                   {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :FORM/CD                              {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :ONT_FORM_ROUTE/CD                    {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :ROUTE/CD                             {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :DT_PAYMENT_CATEGORY/CD               {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :SUPPLIER/CD                          {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :FLAVOUR/CD                           {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :COLOUR/CD                            {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :BASIS_OF_STRNTH/CD                   {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :REIMBURSEMENT_STATUS/CD              {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :SPEC_CONT/CD                         {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :VIRTUAL_PRODUCT_NON_AVAIL/CD         {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :DISCONTINUED_IND/CD                  {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :DF_INDICATOR/CD                      {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :PRICE_BASIS/CD                       {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :LEGAL_CATEGORY/CD                    {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :AVAILABILITY_RESTRICTION/CD          {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :LICENSING_AUTHORITY_CHANGE_REASON/CD {:db/unique :db.unique/identity :db/valueType :db.type/long}

   ;; ingredients
   :INGREDIENT/ISID                      {:db/unique :db.unique/identity :db/valueType :db.type/long}
   :INGREDIENT/ISIDPREV                  {:db/valueType :db.type/long}
   :INGREDIENT/INVALID                   {:db/valueType :db.type/boolean}
   :INGREDIENT/NM                        {:db/valueType :db.type/string}

   ;; BNF extra information (BNF codes, ATC codes etc
   :BNF_DETAILS/BNF                      {:db/valueType :db.type/string}
   :BNF_DETAILS/ATC                      {:db/valueType :db.type/string}
   :BNF_DETAILS/DDD                      {:db/valueType :db.type/double}
   :BNF_DETAILS/DDD_UOMCD                {:db/valueType :db.type/long}
   :BNF_DETAILS/DDD_UOM                  {:db/valueType :db.type/ref}})

(def lookup-references
  "Declarative description of how how individual properties can be supplemented
  by adding a datalog reference using the property and the foreign key specified.

  A nested map of <file-type> <component-type> with a tuple representing
  - property    : name of property to contain the reference
  - foreign-key : the attribute representing the foreign key.

  For example, the :VTMID property of the VMP component is turned into a
  property :VMP/VTM that will reference using the key :PRODUCT/ID."
  {:VMP  {:VMP                        {:VTMID           [:VMP/VTM :PRODUCT/ID]
                                       :BASISCD         [:VMP/BASIS :BASIS_OF_NAME/CD]
                                       :BASIC_PREVCD    [:VMP/BASIC_PREV :BASIS_OF_NAME/CD]
                                       :NMCHANGECD      [:VMP/NMCHANGE :NAMECHANGE_REASON/CD]
                                       :COMBPRODCD      [:VMP/COMBPROD :COMBINATION_PROD_IND/CD]
                                       :PRES_STATCD     [:VMP/PRES_STAT :VIRTUAL_PRODUCT_PRES_STATUS/CD]
                                       :NON_AVAILCD     [:VMP/NON_AVAIL :VIRTUAL_PRODUCT_NON_AVAIL/CD]
                                       :DF_INDCD        [:VMP/DF_IND :DF_INDICATOR/CD]
                                       :UDFS_UOMCD      [:VMP/UDFS_UOM :UNIT_OF_MEASURE/CD]
                                       :UNIT_DOSE_UOMCD [:VMP/UNIT_DOSE_UOM :UNIT_OF_MEASURE/CD]}
          :VIRTUAL_PRODUCT_INGREDIENT {:BASIS_STRNTCD     [:VPI/BASIS_STRNT :BASIS_OF_STRNTH/CD]
                                       :STRNT_NMRTR_UOMCD [:VPI/STRNT_NMRTR_UOM :UNIT_OF_MEASURE/CD]
                                       :STRNT_DNMTR_UOMCD [:VPI/STRNT_DNMTR_UOM :UNIT_OF_MEASURE/CD]
                                       :ISID              [:VPI/IS :INGREDIENT/ISID]}
          :ONT_DRUG_FORM              {:FORMCD [:VMP/ONT_DRUG_FORMS :ONT_FORM_ROUTE/CD]}
          :DRUG_FORM                  {:FORMCD [:VMP/DRUG_FORM :FORM/CD]}
          :DRUG_ROUTE                 {:ROUTECD [:VMP/DRUG_ROUTES :ROUTE/CD]}
          :CONTROL_DRUG_INFO          {:CATCD [:VMP/CONTROL_DRUG_INFO :CONTROL_DRUG_CATEGORY/CD]}}
   :AMP  {:AMP            {:VPID             [:AMP/VP :PRODUCT/ID]
                           :SUPPCD           [:AMP/SUPP :SUPPLIER/CD]
                           :LIC_AUTHCD       [:AMP/LIC_AUTH :LICENSING_AUTHORITY/CD]
                           :LIC_AUTH_PREVCD  [:AMP/AUTH_PREV :LICENSING_AUTHORITY/CD]
                           :LIC_AUTHCHANGECD [:AMP/LIC_AUTHCHANGED :LICENSING_AUTHORITY_CHANGE_REASON/CD]
                           :COMBPRODCD       [:AMP/COMBPROD :COMBINATION_PROD_IND/CD]
                           :FLAVOURCD        [:AMP/FLAVOUR :FLAVOUR/CD]
                           :AVAIL_RESTRICTCD [:AMP/AVAIL_RESTRICT :AVAILABILITY_RESTRICTION/CD]}
          :AP_INGREDIENT  {:UOMCD [:AP_INGREDIENT/UOM :UNIT_OF_MEASURE/CD]
                           :ISID  [:AP_INGREDIENT/IS :INGREDIENT/ISID]}
          :LICENSED_ROUTE {:ROUTECD [:AMP/LICENSED_ROUTES :ROUTE/CD]}
          :AP_INFORMATION {:COLOURCD [:AMP/COLOUR :COLOUR/CD]}}
   :VMPP {:VMPP             {:VPID       [:VMPP/VP :PRODUCT/ID]
                             :QTY_UOMCD  [:VMPP/QTY_UOM :UNIT_OF_MEASURE/CD]
                             :COMBPACKCD [:VMPP/COMBPACK :COMBINATION_PACK_IND/CD]}
          :DRUG_TARIFF_INFO {:PAY_CATCD [:DRUG_TARIFF_INFO/PAY_CAT :DT_PAYMENT_CATEGORY/CD]}
          :COMB_CONTENT     {:CHLDVPPID [:VMPP/CHLDVPP :PRODUCT/ID]}}
   :AMPP {:AMPP                    {:COMBPACKCD  [:AMPP/COMBPACK :COMBINATION_PACK_IND/CD]
                                    :LEGAL_CATCD [:AMPP/LEGAL_CAT :LEGAL_CATEGORY/CD]
                                    :DISCCD      [:AMPP/DISC :DISCONTINUED_IND/CD]
                                    :VPPID       [:AMPP/VPP :PRODUCT/ID]
                                    :APID        [:AMPP/AP :PRODUCT/ID]}
          :APPLIANCE_PACK_INFO     {:REIMB_STATCD     [:APPLIANCE_PACK_INFO/REIMB_STAT :REIMBURSEMENT_STATUS/CD]
                                    :REIMB_STATPREVCD [:APPLIANCE_PACK_INFO/REIMB_STATPREV :REIMBURSEMENT_STATUS/CD]}
          :MEDICINAL_PRODUCT_PRICE {:PRICE_BASISCD [:MEDICINAL_PRODUCT_PRICE/PRICE_BASIS :PRICE_BASIS/CD]}
          :REIMBURSEMENT_INFO      {:SPEC_CONTCD [:REIMBURSEMENT_INFO/SPEC_CONT :SPEC_CONT/CD]}
          :COMB_CONTENT            {:CHLDAPPID [:AMPP/CHLDAPP :PRODUCT/ID]}}
   :BNF  {:VMPS {:DDD_UOMCD [:BNF_DETAILS/DDD_UOM :UNIT_OF_MEASURE/CD]}}})

(defn parse-entity
  "Turn an arbitrary entity into something readily importable into datalog.
  In essence, this namespaces any properties and turns lookups into datalog
  references. It is most useful in parsing complete components or properties
  of a dm+d component that are themselves entities (e.g. ingredients) in which
  there are multiple properties (ingredient, amount etc)."
  [m nspace]
  (let [[file-type component-type] (:TYPE m)]
    (reduce-kv (fn [m k v]
                 (let [k' (keyword nspace (name k))]
                   (if-let [[prop fk] (get-in lookup-references [file-type component-type k])]
                     (assoc m prop [fk v] k' v)             ;; turn any known reference properties into datalog references
                     (assoc m k' v))))
               {}
               (dissoc m :TYPE))))

(defn parse-reference
  "Turn an arbitrary entity into a flattened reference.
   This is most suitable for simple properties such as lookups."
  [m]
  (let [[file-type component-type] (:TYPE m)]
    (reduce-kv (fn [m k v]
                 (if-let [[prop fk] (get-in lookup-references [file-type component-type k])] ;; turn any known reference properties into datalog references
                   (assoc m prop [fk v])                    ;; a datalog reference is a tuple of the foreign key and the value e.g [:PRODUCT/ID 123]
                   m))
               {} (dissoc m :TYPE))))

(defn parse-product [m id-key]
  (let [[_file-type component-type] (:TYPE m)]
    (-> (parse-entity m (name component-type))
        (assoc :PRODUCT/TYPE component-type
               :PRODUCT/ID (get m id-key)))))

(defn parse-lookup
  "Parse the definition of a lookup, or similar. "
  [m]
  (let [[_file-type component-type] (:TYPE m)
        nspace (name component-type)
        m' (dissoc m :ID :TYPE)]
    (-> (reduce-kv (fn [m k v] (assoc m (keyword nspace (name k)) (or v ""))) {} m')
        (assoc :LOOKUP/KIND component-type))))

(defn parse-nested-property
  "Parse a product's set of properties (e.g. VPI) by creating a new entity.
  This is most suitable for complex to-many relationships.
  For example,
  {:TYPE [:VMP :VIRTUAL_PRODUCT_INGREDIENT],
  :VPID 319996000, :ISID 387584000,
  :BASIS_STRNTCD \"0001\", :STRNT_NMRTR_VAL 10.0, :STRNT_NMRTR_UOMCD 258684004}
  will be parsed into:
  {:PRODUCT/ID 319996000
   :VMP/INGREDIENTS { ... }}
  so that the values are nested under the property specified."
  [m entity-name property-name product-key]
  (let [entity-name' (name entity-name)]
    (hash-map property-name (parse-entity m entity-name')
              :PRODUCT/ID (product-key m))))

(defn parse-flat-property
  "Parse a simple to-one or to-many property that is simply a reference type.
  For example,
    {:TYPE [:VMP :DRUG_ROUTE], :VPID 318248001, :ROUTECD 26643006}
  will be parsed into:
    {:VMP/DRUG_ROUTES [:ROUTE/CD 26643006], :PRODUCT/ID 318248001}."
  [m product-key]
  (let [[file-type _] (:TYPE m)
        pkey (product-key m)]
    (-> (parse-reference m)
        (assoc :PRODUCT/ID pkey))))

(defn parse
  "Parse a dm+d component into a map suitable for storing in the datalog store.
  Each component is given a type, a tuple of the file type and the component type.
  We use that tuple to determine how to parse data.
  Parameters:
     - m : dm+d component with :TYPE property a tuple of file and component type."
  [m]
  (match [(:TYPE m)]
         [[:VTM :VTM]] (parse-product m :VTMID)
         [[:VMP :VMP]] (parse-product m :VPID)
         [[:VMP :VIRTUAL_PRODUCT_INGREDIENT]] (parse-nested-property m :VPI :VMP/INGREDIENTS :VPID)
         [[:VMP _]] (parse-flat-property m :VPID)
         [[:AMP :AMP]] (parse-product m :APID)
         [[:AMP :AP_INGREDIENT]] (parse-nested-property m :AP_INGREDIENT :AMP/EXCIPIENTS :APID)
         [[:AMP _]] (parse-flat-property m :APID)
         [[:VMPP :VMPP]] (parse-product m :VPPID)
         [[:VMPP :DRUG_TARIFF_INFO]] (parse-nested-property m :DRUG_TARIFF_INFO :VMPP/DRUG_TARIFF_INFO :VPPID)
         [[:VMPP :COMB_CONTENT]] (parse-flat-property m :PRNTVPPID) ;; note reference to parent is :PRNTVPPID not :VPPID
         [[:VMPP _]] (parse-flat-property m :VPPID)         ;; for all other properties, the FK to the parent is VPPID
         [[:AMPP :AMPP]] (parse-product m :APPID)
         [[:AMPP :APPLIANCE_PACK_INFO]] (parse-nested-property m :APPLIANCE_PACK_INFO :AMPP/APPLIANCE_PACK_INFO :APPID)
         [[:AMPP :DRUG_PRODUCT_PRESCRIB_INFO]] (parse-nested-property m :DRUG_PRODUCT_PRESCRIB_INFO :AMPP/DRUG_PRODUCT_PRESCRIB_INFO :APPID)
         [[:AMPP :MEDICINAL_PRODUCT_PRICE]] (parse-nested-property m :MEDICINAL_PRODUCT_PRICE :AMPP/MEDICINAL_PRODUCT_PRICE :APPID)
         [[:AMPP :REIMBURSEMENT_INFO]] (parse-nested-property m :REIMBURSEMENT_INFO :AMPP/REIMBURSEMENT_INFO :APPID)
         [[:AMPP :COMB_CONTENT]] (parse-flat-property m :PRNTAPPID) ;; for COMB_CONTENT, the parent is PRNTAPPID not :APPID
         [[:AMPP _]] (parse-flat-property m :APPID)
         [[:INGREDIENT :INGREDIENT]] (parse-lookup m)
         [[:LOOKUP _]] (parse-lookup m)
         [[:GTIN :AMPP]] (parse-nested-property m :GTIN_DETAILS :AMPP/GTIN_DETAILS :AMPPID) ;;NB: they use :AMPPID not APPID in the GTIN file, just to be inconsistent
         [[:BNF :VMPS]] (parse-nested-property m :BNF_DETAILS :VMP/BNF_DETAILS :VPID)
         [[:BNF :AMPS]] (parse-nested-property m :BNF_DETAILS :AMP/BNF_DETAILS :APID)
         :else (log/warn "Unknown file type / component type tuple" m)))

(defn ^DmdStore open-store [dir]
  (->DmdStore (d/create-conn dir schema)))

(defn create-store [dir ch & {:keys [batch-size] :or {batch-size 500}}]
  (let [conn (d/create-conn dir schema)
        cpu (.availableProcessors (Runtime/getRuntime))
        ch' (a/chan 50 (partition-all batch-size))]
    (a/pipeline cpu ch' (map #(parse %)) ch)
    (loop [batch (a/<!! ch')]
      (when batch
        (try
          (d/transact! conn batch)
          (catch Exception e
            (throw (ex-info "error during import" {:batch batch} e))))
        (recur (a/<!! ch'))))
    (->DmdStore conn)))

(defn product-type [^DmdStore st id]
  (:PRODUCT/TYPE (d/q '[:find (pull ?e [:PRODUCT/TYPE]) . :in $ ?id :where [?e :PRODUCT/ID ?id]] (d/db (.-conn st)) id)))

(defn ^:private fetch-product*
  "Fetch a single product with the pull syntax pattern specified."
  [^DmdStore st id pattern]
  (d/q '[:find (pull ?e pattern) .
         :in $ ?id pattern
         :where
         [?e :PRODUCT/ID ?id]]
       (d/db (.-conn st))
       id
       pattern))

(defn fetch-vtm [^DmdStore st vtmid]
  (fetch-product* st vtmid '[*]))

(def vmp-properties
  ['* {:VMP/VTM               '[*]
       :VMP/BASIS             '[*]
       :VMP/UNIT_DOSE_UOM     '[*]
       :VMP/UDFS_UOM          '[*]
       :VMP/DRUG_ROUTES       [:ROUTE/CD :ROUTE/DESC]
       :VMP/DRUG_FORM         [:FORM/CD :FORM/DESC]
       :VMP/NON_AVAIL         [:VIRTUAL_PRODUCT_NON_AVAIL/CD :VIRTUAL_PRODUCT_NON_AVAIL/DESC]
       :VMP/DF_IND            [:DF_INDICATOR/CD :DF_INDICATOR/DESC]
       :VMP/CONTROL_DRUG_INFO [:CONTROL_DRUG_CATEGORY/CD :CONTROL_DRUG_CATEGORY/DESC]
       :VMP/PRES_STAT         [:VIRTUAL_PRODUCT_PRES_STATUS/CD :VIRTUAL_PRODUCT_PRES_STATUS/DESC]
       :VMP/INGREDIENTS       ['*
                               {:VPI/IS              '[*]
                                :VPI/BASIS_STRNT     '[*]
                                :VPI/STRNT_NMRTR_UOM '[*]
                                :VPI/STRNT_DNMTR_UOM '[*]}]
       :VMP/NMCHANGE          '[*]
       :VMP/ONT_DRUG_FORMS    '[*]
       :VMP/BNF_DETAILS       ['* {:BNF_DETAILS/DDD_UOM '[*]}]}])

(def amp-properties
  ['* {:AMP/EXCIPIENTS      ['* {:AP_INGREDIENT/IS '[*]}]
       :AMP/AVAIL_RESTRICT  '[*]
       :AMP/LICENSED_ROUTES '[*]
       :AMP/SUPP            '[*]
       :AMP/LIC_AUTH        '[*]
       :AMP/VP              vmp-properties}])

(def vmpp-properties
  ['*
   {:VMPP/QTY_UOM          ['*]
    :VMPP/DRUG_TARIFF_INFO ['*]
    :VMPP/VP               vmp-properties}])

(def ampp-properties
  ['* {:AMPP/LEGAL_CAT               ['*]
       :AMPP/GTIN_DETAILS            ['*]
       :AMPP/DISC                    ['*]
       :AMPP/MEDICINAL_PRODUCT_PRICE ['* {:MEDICINAL_PRODUCT_PRICE/PRICE_BASIS '[*]}]
       :AMPP/REIMBURSEMENT_INFO      ['*]
       :AMPP/VPP                     vmpp-properties
       :AMPP/AP                      amp-properties}])


(defn fetch-vmp [^DmdStore st vpid]
  (fetch-product* st vpid vmp-properties))

(defn fetch-amp [^DmdStore st apid]
  (fetch-product* st apid amp-properties))

(defn fetch-vmpp [^DmdStore st vppid]
  (fetch-product* st vppid vmpp-properties))

(defn fetch-ampp [^DmdStore st appid]
  (fetch-product* st appid ampp-properties))

(defn fetch-product [^DmdStore st id]
  (let [kind (product-type st id)]
    (case kind
      :VTM (fetch-vtm st id)
      :VMP (fetch-vmp st id)
      :AMP (fetch-amp st id)
      :VMPP (fetch-vmpp st id)
      :AMPP (fetch-ampp st id)
      nil)))

(defn fetch-lookup
  "Returns the lookup for the kind specified.
  Parameters:
  - st    : dm+d store
  - kind  : kind, e.g. :BASIS_OF_NAME :FLAVOUR  :UNIT_OF_MEASURE"
  [^DmdStore st nm]
  (->> (d/q '[:find [(pull ?e [*]) ...]
              :in $ ?kind
              :where
              [?e :LOOKUP/KIND ?kind]]
            (d/db (.-conn st))
            (keyword nm))
       (map #(dissoc % :db/id :LOOKUP/KIND))))

(defn results-for-eids [^DmdStore st eids]
  (d/pull-many (d/db (.-conn st)) [:PRODUCT/ID :PRODUCT/TYPE :VTM/NM :VMP/NM :AMP/NM :VMPP/NM :AMPP/NM] eids))

(defn eids->ids
  "Return a sequence of product identifiers for the eids specified."
  [^DmdStore st eids]
  (map :PRODUCT/ID (d/pull-many (d/db (.-conn st)) '[:PRODUCT/ID] eids)))

(defn vmp-eids-for-vtmid [^DmdStore st vtmid]
  (d/q '[:find [?e ...]
         :in $ ?vtmid
         :where
         [?e :VMP/VTMID ?vtmid]]
       (d/db (.-conn st))
       vtmid))

(defn amp-eids-for-vtmid [^DmdStore st vtmid]
  (d/q '[:find [?amp ...]
         :in $ ?vtmid
         :where
         [?vtm :VTM/VTMID ?vtmid]
         [?vmp :VMP/VTM ?vtm]
         [?amp :AMP/VP ?vmp]]
       (d/db (.-conn st))
       vtmid))

(defn vtm-eid-for-vmpid [^DmdStore st vmpid]
  (d/q '[:find ?vtm .
         :in $ ?vmpid
         :where
         [?vmp :VMP/VPID ?vmpid]
         [?vmp :VMP/VTM ?vtm]]
       (d/db (.-conn st))
       vmpid))

(defn vtm-eids-for-vmp-eids
  [^DmdStore st vmp-eids]
  (d/q '[:find [?vtm ...]
         :in $ [?vmp ...]
         :where
         [?vmp :VMP/VTM ?vtm]]
       (d/db (.-conn st))
       vmp-eids))

(defn amp-eids-for-vmp-eids
  [^DmdStore st vmp-eids]
  (d/q '[:find [?amp ...]
         :in $ [?vmp ...]
         :where
         [?amp :AMP/VP ?vmp]]
       (d/db (.-conn st))
       vmp-eids))

(defn amp-eids-for-vmpid [^DmdStore st vmpid]
  (d/q '[:find [?amp ...]
         :in $ ?vmpid
         :where
         [?vmp :VMP/VPID ?vmpid]
         [?amp :AMP/VP ?vmp]]
       (d/db (.-conn st))
       vmpid))

(defn vtm-eid-for-ampid [^DmdStore st ampid]
  (d/q '[:find ?vtm .
         :in $ ?ampid
         :where
         [?amp :AMP/APID ?ampid]
         [?amp :AMP/VP ?vmp]
         [?vmp :VMP/VTM ?vtm]]
       (d/db (.-conn st))
       ampid))

(defn vmp-eid-for-ampid [^DmdStore st ampid]
  (d/q '[:find ?vmp .
         :in $ ?ampid
         :where
         [?amp :AMP/APID ?ampid]
         [?amp :AMP/VP ?vmp]]
       (d/db (.-conn st))
       ampid))

(defn vmp-eids-from-atc
  "Returns VMP entity ids for the ATC code regular expression.
  Parameters:
   - st   : dm+d store
   - re-atc : regular expression (e.g. #\"L04AX.*\").

  It is usual to use a prefix match match for the ATC given its code structure.

  The Anatomical Therapeutic Chemical (ATC) code: a unique code assigned to a
  medicine according to the organ or system it works on and how it works. The
  classification system is maintained by the World Health Organization (WHO).

  The dm+d ATC mapping only includes VMPs, so depending on usage, extending the
  codeset to include the appropriate other dm+d structures might be required."
  [^DmdStore st ^Pattern re-atc]
  (d/q '[:find [?e ...]
         :in $ ?atc-regexp
         :where
         [?bnf :BNF_DETAILS/ATC ?atc]
         [?e :VMP/BNF_DETAILS ?bnf]
         [(re-matches ?atc-regexp ?atc)]]
       (d/db (.-conn st))
       re-atc))

(def supported-product-types-for-atc-map
  #{:VTM :VMP :AMP})

(defn product-eids-from-atc
  ([^DmdStore st ^Pattern re-atc] (product-eids-from-atc st re-atc supported-product-types-for-atc-map))
  ([^DmdStore st ^Pattern re-atc product-types]
   (when-not (set/subset? product-types supported-product-types-for-atc-map)
     (throw (ex-info "unsupported product-types for ATC mapping" {:requested product-types :supported supported-product-types-for-atc-map})))
   (let [vmp-eids (vmp-eids-from-atc st re-atc)
         vtm-eids (when (product-types :VTM) (vtm-eids-for-vmp-eids st vmp-eids))
         amp-eids (when (product-types :AMP) (amp-eids-for-vmp-eids st vmp-eids))]
     (concat (when (contains? product-types :VMP) vmp-eids)
             vtm-eids amp-eids))))

(defn atc->ecl
  "Convert an ATC code regexp into a SNOMED CT expression that will identify all
  products relating to that code.

  Not all VMPs have a VTM, but a given VTM will subsume all VTMs, VMPs and AMPs
  in the SNOMED drug model.

  Unfortunately, while the UK SNOMED drug extension includes trade family
  entities, dm+d does not. This is unfortunate. In order to identify the TF
  concept for any given AMP, we have to use an ECL expression of the form
  (>'amp-concept-id' AND <9191801000001103|Trade Family|) to identify parent
  concepts in the hierarchy up to and not including the TF concept itself.

  It would not be usual to want to include VMPP or AMPP, but you can include
  if required. All AMPPs are subsumed by VMPPs, so we simply add clauses to
  include VMPPs and descendants for each VMP using the 'Has VMP' relationship."
  [^DmdStore st ^Pattern re-atc & {:keys [include-product-packs?] :or {include-product-packs? false}}]
  (let [vmp-eids (vmp-eids-from-atc st re-atc)
        vmp-ids (eids->ids st vmp-eids)
        vmps (map #(str "<<" %) vmp-ids)
        vtms (map #(str "<<" %) (eids->ids st (vtm-eids-for-vmp-eids st vmp-eids)))
        amp-ids (eids->ids st (amp-eids-for-vmp-eids st vmp-eids))
        ;; for TFs, we ask for Trade family children that are parents of each AMP:
        tfs (map (fn [ampid] (str "<<(>" ampid " AND <9191801000001103)")) amp-ids)
        ;; for product-packs, we ask for UK products that 'Has VMP' of each VMP we matched:
        pp (when include-product-packs? (map (fn [vmpid] (str "<<(<8653601000001108:10362601000001103=" vmpid ")")) vmp-ids))]
    (str/join " OR " (concat vmps vtms tfs pp))))

(defn product-by-name
  "Simple search by name.
  Warning: this is not designed for operational use as text search is slow - it
  will take ~500ms. It is intended for testing and exploration purposes only."
  [st re-nm]
  (d/q '[:find ?id ?nm
         :in $ ?s
         :where
         [?e :PRODUCT/ID ?id]
         (or
           [?e :VTM/NM ?nm]
           [?e :VMP/NM ?nm]
           [?e :AMP/NM ?nm])
         [(re-matches ?s ?nm)]]
       (d/db (.-conn st))
       re-nm))

(defmulti vtms "Return the entity ids for the VTMs associated with this product."
          (fn [^DmdStore _store product] (:PRODUCT/TYPE product)))

(defmethod vtms :VTM [store vtm]
  [(if-let [db-id (:db/id vtm)]
     db-id
     (d/q '[:find ?e .
            :in $ ?vtmid
            :where [?e :VTM/VTMID ?vtmid]]
          (d/db (.-conn store))
          (:PRODUCT/ID vtm)))])

(defmethod vtms :VMP [store vmp]
  (when-let [vtm-eid (vtm-eid-for-vmpid store (:PRODUCT/ID vmp))]
    [vtm-eid]))

(defmethod vtms :AMP [store amp]
  (when-let [vtm-eid (vtm-eid-for-ampid store (:PRODUCT/ID amp))]
    [vtm-eid]))

(defmulti vmps "Returns the VMPs associated with this product."
          (fn [^DmdStore _store product] (:PRODUCT/TYPE product)))

(defmethod vmps :VTM [store vtm]
  (vmp-eids-for-vtmid store (:PRODUCT/ID vtm)))

(defmethod vmps :VMP [store vmp]
  [(if-let [db-id (:db/id vmp)]
     db-id
     (d/q '[:find ?e .
            :in $ ?vmpid
            :where [?e :VMP/VPID ?vmpid]]
          (d/db (.-conn store))
          (:PRODUCT/ID vmp)))])

(defmethod vmps :AMP [store amp]
  (when-let [vmp-eid (vmp-eid-for-ampid store (:PRODUCT/ID amp))]
    [vmp-eid]))

(defmulti amps "Returns identifiers for the AMPs associated with this product."
          (fn [^DmdStore _store product] (:PRODUCT/TYPE product)))

(defmethod amps :VTM [store amp]
  (amp-eids-for-vtmid store (:PRODUCT/ID amp)))

(defmethod amps :VMP [store amp]
  (amp-eids-for-vmpid store (:PRODUCT/ID amp)))

(defmethod amps :AMP [store amp]
  [(if-let [db-id (:db/id amp)]
     db-id
     (d/q '[:find ?e .
            :in $ ?ampid
            :where [?e :AMP/APID ?ampid]]
          (d/db (.-conn store))
          (:PRODUCT/ID amp)))])

(defmulti atc-code "Return the ATC code associated with this product."
          (fn [^DmdStore _store product] (:PRODUCT/TYPE product)))

(defmethod atc-code :VTM [store vtm]
  (d/q '[:find ?atc .
         :in $ ?vtmid
         :where
         [?bnf :BNF_DETAILS/ATC ?atc]
         [?e :VMP/BNF_DETAILS ?bnf]
         [?e :VMP/VTMID ?vtmid]]
       (d/db (.-conn store))
       (:PRODUCT/ID vtm)))

(defmethod atc-code :VMP [store vmp]
  (if-let [atc (get-in vmp [:VMP/BNF_DETAILS :BNF_DETAILS/ATC])]
    atc
    (d/q '[:find ?atc .
           :in $ ?vmpid
           :where
           [?bnf :BNF_DETAILS/ATC ?atc]
           [?e :VMP/BNF_DETAILS ?bnf]
           [?e :VMP/VPID ?vmpid]]
         (d/db (.-conn store))
         (:PRODUCT/ID vmp))))

(defmethod atc-code :AMP [store amp]
  (if-let [atc (get-in amp [:AMP/VP :VMP/BNF_DETAILS :BNF_DETAILS/ATC])]
    atc
    (d/q '[:find ?atc .
           :in $ ?ampid
           :where
           [?bnf :BNF_DETAILS/ATC ?atc]
           [?vmp :VMP/BNF_DETAILS ?bnf]
           [?amp :AMP/VP ?vmp]
           [?amp :AMP/APID ?ampid]]
         (d/db (.-conn store))
         (:PRODUCT/ID amp))))


(comment
  (def lookup-example {:TYPE [:LOOKUP :NAMECHANGE_REASON], :CD "0003", :DESC "Basis of name changed", :ID :NAMECHANGE_REASON-0003})
  (parse lookup-example)
  (parse {:TYPE [:VTM :VTM], :VTMID 68088000, :NM "Acebutolol"})
  (parse {:BASISCD         "0007",                          ;; example of a VMP:
          :UNIT_DOSE_UOMCD 415818006,
          :VTMID           39482111000001100,
          :ABBREVNM        "Sod pyrophosphate 20mg",
          :UDFS_UOMCD      415818006,
          :VPID            39488411000001107,
          :TYPE            [:VMP :VMP],
          :NM              "Sodium pyrophosphate decahydrate 20mg kit for radiopharmaceutical preparation",
          :PRES_STATCD     "0001",
          :DF_INDCD        "1",
          :UDFS            1})

  ;;
  (def dir "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001")
  (def conn (d/create-conn "wibble.db" schema))
  (def conn (d/create-conn "dmd-2021-08-30.db" schema))
  (def st (->DmdStore conn))

  (require '[com.eldrix.dmd.import :as dim])
  (def ch1 (a/chan 500))
  (def ch2 (a/chan 50 (partition-all 5000)))
  (a/thread (dim/stream-dmd dir ch1 :include #{:AMPP}))
  (a/pipeline (.availableProcessors (Runtime/getRuntime)) ch2 (map #(parse %)) ch1)
  (a/<!! ch2)

  (def ch (a/chan))
  (def ch (a/chan 5 (comp (map #(parse %)) (partition-all 1))))

  (a/thread (dim/stream-dmd dir ch1 :include #{:LOOKUP}))
  (a/thread (dim/stream-dmd dir ch :include #{:INGREDIENT}))
  (a/thread (dim/stream-dmd dir ch :include #{:GTIN}))
  (a/thread (dim/stream-dmd "/Users/mark/Downloads/week272021-r2_3-BNF" ch))
  (a/thread (dim/stream-dmd "/var/folders/w_/s108lpdd1bn84sntjbghwz3w0000gn/T/trud15801406225560397483/week352021-r2_3-GTIN-zip/" ch))
  (a/thread (dim/stream-dmd "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001" ch :include #{:AMPP}))
  (d/transact! conn (a/<!! ch))
  (a/<!! ch)
  (def batch (a/<!! ch))
  batch
  (map parse (a/<!! ch))
  (take 2 (a/<!! ch))
  (def batch (a/<!! ch))
  (map parse batch)
  ;; this loop imports data assuming the channel includes parsing in the transducer
  (loop [batch (a/<!! ch)]
    (when batch
      (println batch)
      (d/transact! conn batch)
      (recur (a/<!! ch))))

  ;; this loop imports data explicitly parsing before import - use with channel with no parse transducer
  (loop [batch (a/<!! ch)]
    (when batch
      ;(println "processing batch" (map parse batch))
      ;(println (zipmap (keys (first batch)) (map type (vals (first batch)))))
      (d/transact! conn (map parse batch))
      (recur (a/<!! ch))))

  (def lookups (dim/get-component dir :LOOKUP :COMBINATION_PACK_IND))
  (def lookups (dim/get-component dir :LOOKUP :ROUTE))
  (def lookups (dim/get-component dir :LOOKUP :NAMECHANGE_REASON))
  (take 4 lookups)
  (map parse lookups)
  (def ingreds (dim/get-component dir :INGREDIENT :INGREDIENT))
  (take 5 (map parse ingreds))

  (def vtms (dim/get-component dir :VTM :VTM))
  (take 5 (reverse (map parse vtms)))

  (def vmps (dim/get-component dir :VMP :VMP))
  (take 5 (map parse vmps))

  (def vpis (dim/get-component dir :VMP :VIRTUAL_PRODUCT_INGREDIENT))
  (take 2 vpis)
  (take 5 (reverse (map parse vpis)))

  (def vmp-ont-drug-forms (dim/get-component dir :VMP :ONT_DRUG_FORM))
  (take 5 vmp-ont-drug-forms)
  (take 2 (map parse vmp-ont-drug-forms))

  (def x (dim/get-component dir :VMP :DRUG_ROUTE))
  (take 5 x)
  (take 5 (map parse x))

  (def x (dim/get-component dir :VMPP :DRUG_TARIFF_INFO))
  (take 5 x)
  (take 5 (map #(parse-nested-property % "DRUG_TARIFF_INFO" :VMPP/DRUG_TARIFF_INFO :VPPID) x))
  (parse-product x :VPID)
  (parse x)
  (parse (a/<!! ch))
  (d/transact! conn [(parse x)])
  (d/transact! conn [(parse (a/<!! ch))])


  (def mpp (dim/get-component "/var/folders/w_/s108lpdd1bn84sntjbghwz3w0000gn/T/trud2465267306253668332" :AMPP :MEDICINAL_PRODUCT_PRICE))
  (take 5 (map parse mpp))

  ;; get all codes for a given lookup
  (time (d/q '[:find ?code ?desc
               :where
               [?e :BASIS_OF_NAME/CD ?code]
               [?e :BASIS_OF_NAME/DESC ?desc]]
             (d/db conn)))

  (d/touch (d/entity (d/db conn) (d/q '[:find ?e .
                                        :where
                                        [?e :PRODUCT/ID 319996000]]
                                      (d/db conn))))

  (map :VMP/NM (map #(d/entity (d/db conn) %) (d/q '[:find [?e ...]
                                                     :where
                                                     [?e :VMP/VTMID 108537001]]
                                                   (d/db conn))))
  (d/q '[:find (pull ?e [*])
         :where
         [?e :INGREDIENT/ISID 391730008]]
       (d/db conn))

  (time (d/q '[:find (pull ?e [*
                               {:VMP/UNIT_DOSE_UOM     [*]
                                :VMP/UDFS_UOM          [*]
                                :VMP/DRUG_ROUTES       [:ROUTE/CD :ROUTE/DESC]
                                :VMP/DRUG_FORMS        [:FORM/CD :FORM/DESC]
                                :VMP/DF_IND            [:DF_INDICATOR/CD :DF_INDICATOR/DESC]
                                :VMP/CONTROL_DRUG_INFO [:CONTROL_DRUG_CATEGORY/CD :CONTROL_DRUG_CATEGORY/DESC]
                                :VMP/PRES_STAT         [:VIRTUAL_PRODUCT_PRES_STATUS/CD :VIRTUAL_PRODUCT_PRES_STATUS/DESC]
                                :VMP/INGREDIENTS       [* {:VPI/IS [*]} {:VPI/BASIS_STRNT [*]}]}]) .
               :where
               [?e :PRODUCT/ID 7322211000001104]]
             (d/db conn)))


  ;; generate an extended AMP for all AMPs of co-amoxiclav VMP (7322211000001104)
  (d/q '[:find (pull ?e [*
                         {:AMP/EXCIPIENTS [* {:AP_INGREDIENT/IS [*]}]}
                         {:AMP/AVAIL_RESTRICT [*]}
                         {:AMP/LICENSED_ROUTES [*]}
                         {:AMP/SUPP [*]}
                         {:AMP/LIC_AUTH [*]}
                         {:AMP/VP [*
                                   {:VMP/INGREDIENTS [* {:VPI/IS [*]}]}
                                   {:VMP/PRES_STAT [*]}
                                   {:VMP/CONTROL_DRUG_INFO [*]}
                                   {:VMP/DRUG_ROUTES [*]}
                                   {:VMP/ONT_DRUG_FORMS [:ONT_FORM_ROUTE/DESC]}
                                   {:VMP/VTM [*]}]}])
         :where
         [?e :AMP/VP [:PRODUCT/ID 7322211000001104]]]
       (d/db conn))

  (d/q '[:find (pull ?e [*
                         {:VMPP/QTY_UOM [*]}
                         {:VMPP/DRUG_TARIFF_INFO [* {:DRUG_TARIFF_INFO/PAY_CAT [*]}]}])
         :where
         [?e :VMPP/VP [:PRODUCT/ID 7322211000001104]]]
       (d/db conn))

  ;; find all VMPs containing amoxicillin trihydrate
  (d/q '[:find (pull ?vmp [:PRODUCT/ID :VMP/NM])
         :where
         [?vmp :VMP/INGREDIENTS ?vpi]
         [?vpi :VPI/IS ?ingred]
         [?ingred :INGREDIENT/NM "Amoxicillin trihydrate"]]
       (d/db conn))

  ;; find all AMPs for a given VTM
  (d/q '[:find (pull ?amp [*])
         :where
         [?amp :AMP/VP ?vmp]
         [?vmp :VMP/VTM ?vtm]
         [?vtm :VTM/NM "Amlodipine"]]
       (d/db conn))

  (d/q '[:find ?code ?desc
         :where
         [?e :FORM/CD ?code]
         [?e :FORM/DESC ?desc]]
       (d/db conn))

  ;; map ATC code to product identifiers (only VMPs are given an ATC map...
  (d/q '[:find [?id ...]
         :where
         [?e :PRODUCT/ID ?id]
         (or [?e :VMP/BNF_DETAILS ?bnf]
             [?e :AMP/BNF_DETAILS ?bnf])
         [?bnf :BNF_DETAILS/ATC ?atc]
         [(re-matches #"L04AX.*" ?atc)]]
       (d/db conn))

  (time (vmps-from-atc st #"N02BA01.*"))

  (time (d/q '[:find ?code ?desc
               :where
               [?e :BASIS_OF_NAME/CD ?code]
               [?e :BASIS_OF_NAME/DESC ?desc]]
             (d/db conn)))

  (time (d/q '[:find (pull ?e [*])
               :where
               [?e :BASIS_OF_NAME/CD _]
               [?e :BASIS_OF_NAME/DESC _]]
             (d/db conn)))

  (time (d/q '[:find (pull ?e [*])
               :where
               [?e :LOOKUP/KIND :BASIS_OF_NAME]]
             (d/db conn)))


  (time (d/pull-many (d/db (.-conn st)) '[:PRODUCT/ID :PRODUCT/TYPE :VTM/NM :VMP/NM :AMP/NM :VMPP/NM :AMPP/NM]
                     ))



  (def conn (d/create-conn "dmd-2021-09-06.db" schema))
  (def st (->DmdStore conn))
  (fetch-product st 13275011000001101)
  (atc->ecl st #"L04AX0." :include-product-packs? true)
  (results-for-eids st (amps st (fetch-product st 109143003)))
  (results-for-eids st (vmp-eids-from-atc st #"L03AX13"))
  (results-for-eids st (d/q '[:find [?vtm ...]
                              :in $ [?vmp ...]
                              :where
                              [?vmp :VMP/VTM ?vtm]]
                            (d/db (.-conn st))
                            (vmp-eids-from-atc st #"L03.*")))
  (results-for-eids st (vtms st (fetch-product st 36051411000001106)))
  (results-for-eids st [8684])
  (fetch-lookup st :PRICE_BASIS)
  (vmps-from-atc st #"L04.*")
  (atc->ecl st #"L03AX13")
  (time (product-type st 24408011000001101))
  (def id 24408011000001101)
  (time (:PRODUCT/TYPE (d/q '[:find (pull ?e [*]) . :in $ ?id :where [?e :PRODUCT/ID ?id]] (d/db (.-conn st)) id)))
  (time (:PRODUCT/TYPE (d/q '[:find (pull ?e [:PRODUCT/TYPE]) . :in $ ?id :where [?e :PRODUCT/ID ?id]] (d/db (.-conn st)) id)))
  (time (d/q '(:find (pull ?e [:PRODUCT/ID]) . :in $ ?id :where [?e :PRODUCT/ID ?id]) (d/db (.-conn st)) 24408011000001101))
  (time (d/pull (d/db (.-conn st)) [:PRODUCT/TYPE :PRODUCT/ID] 14733))
  )