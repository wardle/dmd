(ns com.eldrix.dmd.parse
  "Parse dm+d data into a format suitable for ingestion into a datalog database."
  (:require [clojure.core.match :refer [match]]
            [clojure.tools.logging.readable :as log]))

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
         [[:VMPP _]] (parse-flat-property m :VPPID)              ;; for all other properties, the FK to the parent is VPPID
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
