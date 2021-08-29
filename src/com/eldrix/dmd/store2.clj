(ns com.eldrix.dmd.store2
  "A datalog store for dm+d"
  (:require [clojure.core.match :refer [match]]
            [clojure.tools.logging.readable :as log]
            [datalevin.core :as d]
            [com.eldrix.dmd.import :as dim]
            [clojure.string :as str]))


(def schema
  "The datalog-based schema is a close representation to the source dm+d data
  structures. The key characteristics are:
   - all products are given a :PRODUCT/ID property and a :PRODUCT/TYPE property.
   - properties are namespaced using a code representing the filename from which
   they were derived (e.g. :VTM :VMP :AMP :LOOKUP etc)
   - lookups are referenced by their code (e.g. :BASIS_OF_NAME/CD \"0003\") and
   the source entity has a property :VMP/BASIS based on the original reference
   (e.g. :VMP/BASISCD)
   - complex to-many relationships (e.g. ingredients of a product) are stored as
   entities themselves, with a unique identifier generated to prevent
   duplicates. Simpler to-one or to-many relationships are simply added to the
   parent entity (e.g. drug forms). These are given a property in the singular
   for to-one relationships, and in the plural for to-many relationships."
  {:PRODUCT/ID                           {:db/unique    :db.unique/identity
                                          :db/valueType :db.type/long}
   ;; VTM
   :VTM/VTMID                            {:db/valueType :db.type/long}
   :VTM/INVALID                          {:db/valueType :db.type/boolean}
   :VTM/VTMIDPREV                        {:db/valueType :db.type/long}

   ;; VMP
   :VMP/VPID                             {:db/valueType :db.type/long}
   :VMP/VTM                              {:db/valueType :db.type/ref}
   :VMP/INVALID                          {:db/valueType :db.type/boolean}
   :VMP/BASIS                            {:db/valueType :db.type/ref}
   :VMP/BASIS_PREV                       {:db/valueType :db.type/ref}
   :VMP/NMCHANGE                         {:db/valueType :db.type/ref}
   :VMP/COMBPROD                         {:db/valueType :db.type/ref}
   :VMP/PRES_STAT                        {:db/valueType :db.type/ref}
   :VMP/NON_AVAIL                        {:db/valueType :db.type/ref}
   :VMP/DF_IND                           {:db/valueType :db.type/ref}
   :VMP/UDFS                             {:db/valueType :db.type/double}
   :VMP/INGREDIENTS                      {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :VMP/ONT_FORMS                        {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :VMP/DRUG_FORMS                       {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :VMP/DRUG_ROUTES                      {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :VMP/CONTROL_DRUG_INFO                {:db/valueType   :db.type/ref ;; TODO: need to check whether needs to be to-many cardinality?
                                          :db/cardinality :db.cardinality/one}

   ;; VMP - VPIs
   :VPI/PRODUCT                          {:db/valueType :db.type/ref}
   :VPI/ISID                             {:db/valueType :db.type/long}
   :VPI/IS                               {:db/valueType :db.type/ref}
   :VPI/BASIS_STRNT                      {:db/valueType :db.type/ref}
   :VPI/BS_SUB                           {:db/valueType :db.type/ref}
   :VPI/STRNT_NMRTR_VAL                  {:db/valueType :db.type/double}
   :VPI/STRNT_NMRTR_UOMCD                {:db/valueType :db.type/long}

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
   :AMP/LIC_AUTHCD                       {:db/valueType :db.type/string}
   :AMP/LIC_AUTH                         {:db/valueType :db.type/ref}
   :AMP/LIC_AUTH_PREVCD                  {:db/valueType :db.type/string}
   :AMP/LIC_AUTH_PREV                    {:db/valueType :db.type/ref}
   :AMP/LIC_AUTHCHANGE                   {:db/valueType :db.type/ref}
   :AMP/COMBPROD                         {:db/valueType :db.type/ref}
   :AMP/FLAVOURCD                        {:db/valueType :db.type/ref}
   :AMP/EMA                              {:db/valueType :db.type/boolean}
   :AMP/PARALLEL_IMPORT                  {:db/valueType :db.type/boolean}
   :AMP/AVAIL_RESTRICT                   {:db/valueType :db.type/ref}
   :AMP/EXCIPIENTS                       {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :AMP/LICENSED_ROUTES                  {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :AMP/COLOUR                           {:db/valueType :db.type/ref}

   ;; AMP - AP-INGREDIENT   (excipients)
   :AP_INGREDIENT/APID                   {:db/valueType :db.type/long}
   :AP_INGREDIENT/ISID                   {:db/valueType :db.type/long}
   :AP_INGREDIENT/IS                     {:db/valueType :db.type/ref}
   :AP_INGREDIENT/STRNTH                 {:db/valueType :db.type/double}
   :AP_INGREDIENT/UOMCD                  {:db/valueType :db.type/long}
   :AP_INGREDIENT/UOM                    {:db/valueType :db.type/ref}

   ;; lookups
   :COMBINATION_PACK_IND/CD              {:db/unique :db.unique/identity}
   :COMBINATION_PROD_IND/CD              {:db/unique :db.unique/identity}
   :BASIS_OF_NAME/CD                     {:db/unique :db.unique/identity}
   :NAMECHANGE_REASON/CD                 {:db/unique :db.unique/identity}
   :VIRTUAL_PRODUCT_PRES_STATUS/CD       {:db/unique :db.unique/identity}
   :CONTROL_DRUG_CATEGORY/CD             {:db/unique :db.unique/identity}
   :LICENSING_AUTHORITY/CD               {:db/unique :db.unique/identity}
   :UNIT_OF_MEASURE/CD                   {:db/unique :db.unique/identity}
   :FORM/CD                              {:db/unique :db.unique/identity}
   :ONT_FORM_ROUTE/CD                    {:db/unique :db.unique/identity}
   :ROUTE/CD                             {:db/unique    :db.unique/identity
                                          :db/valueType :db.type/long}
   :DT_PAYMENT_CATEGORY/CD               {:db/unique :db.unique/identity}
   :SUPPLIER/CD                          {:db/unique :db.unique/identity}
   :FLAVOUR/CD                           {:db/unique :db.unique/identity}
   :COLOUR/CD                            {:db/unique :db.unique/identity}
   :BASIS_OF_STRNTH/CD                   {:db/unique :db.unique/identity}
   :REIMBURSEMENT_STATUS/CD              {:db/unique :db.unique/identity}
   :SPEC_CONT/CD                         {:db/unique :db.unique/identity}
   :VIRTUAL_PRODUCT_NON_AVAIL/CD         {:db/unique :db.unique/identity}
   :DISCONTINUED_IND/CD                  {:db/unique :db.unique/identity}
   :DF_INDICATOR/CD                      {:db/unique :db.unique/identity}
   :PRICE_BASIS/CD                       {:db/unique :db.unique/identity}
   :LEGAL_CATEGORY/CD                    {:db/unique :db.unique/identity}
   :AVAILABILITY_RESTRICTION/CD          {:db/unique :db.unique/identity}
   :LICENSING_AUTHORITY_CHANGE_REASON/CD {:db/unique :db.unique/identity}

   ;; ingredients
   :INGREDIENT/ISID                      {:db/unique :db.unique/identity}})

(def lookup-references
  "Defines how individual properties can be supplemented by adding a datalog
  reference using the property and the foreign key specified.
  A nested map of <file-type> <component-type> with a tuple representing
  - property    : name of property to contain the reference
  - foreign-key : the attribute representing the foreign key."
  {:VMP {:VMP                        {:VTMID        [:VMP/VTM :PRODUCT/ID]
                                      :BASISCD      [:VMP/BASIS :BASIS_OF_NAME/CD]
                                      :BASIC_PREVCD [:VMP/BASIC_PREV :BASIS_OF_NAME/CD]
                                      :NMCHANGECD   [:VMP/NMCHANGE :NAMECHANGE_REASON/CD]
                                      :COMBPRODCD   [:VMP/COMBPROD :COMBINATION_PROD_IND/CD]
                                      :PRES_STATCD  [:VMP/PRES_STAT :VIRTUAL_PRODUCT_PRES_STATUS/CD]
                                      :NON_AVAILCD  [:VMP/NON_AVAIL :VIRTUAL_PRODUCT_NON_AVAIL/CD]
                                      :DF_INDCD     [:VMP/DF_IND :DF_INDICATOR/CD]}
         :VIRTUAL_PRODUCT_INGREDIENT {:BASIS_STRNTCD [:VPI/BASIS_STRNT :BASIS_OF_STRNTH/CD]
                                      :ISID          [:VPI/IS :INGREDIENT/ISID]}
         :ONT_DRUG_FORM              {:FORMCD [:VMP/ONT_DRUG_FORMS :ONT_FORM_ROUTE/CD]}
         :DRUG_FORM                  {:FORMCD [:VMP/DRUG_FORMS :FORM/CD]}
         :DRUG_ROUTE                 {:ROUTECD [:VMP/DRUG_ROUTES :ROUTE/CD]}
         :CONTROL_DRUG_INFO          {:CATCD [:VMP/CONTROL_DRUG_INFO :CONTROL_DRUG_CATEGORY/CD]}}
   :AMP {:AMP            {:VPID             [:AMP/VP :PRODUCT/ID]
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
         :AP_INFORMATION {:COLOURCD [:AMP/COLOUR :COLOUR/CD]}}})

(defn parse-entity [m nspace]
  (let [[file-type component-type] (:TYPE m)]
    (reduce-kv (fn [m k v]
                 (let [k' (keyword nspace (name k))]
                   (if-let [[prop fk] (get-in lookup-references [file-type component-type k])] ;; turn any known reference properties into datalog references
                     (assoc m prop [fk v] k' v)             ;; a datalog reference is a tuple of the foreign key and the value e.g [:PRODUCT/ID 123]
                     (assoc m k' v))))
               {}
               (dissoc m :TYPE))))

(defn parse-product [m id-key]
  (let [[file-type component-type] (:TYPE m)]
    (-> (parse-entity m (name component-type))
        (assoc :PRODUCT/TYPE component-type
               :PRODUCT/ID (get m id-key)))))

(defn parse-lookup [m]
  (let [[file-type component-type] (:TYPE m)
        nspace (name component-type)
        m' (dissoc m :TYPE :ID)]
    (reduce-kv (fn [m k v] (assoc m (keyword nspace (name k)) (or v ""))) {} m')))

(defn parse-bnf [m]
  )

(defn parse-extended-property
  "Parse a product's set of properties (e.g. VPI) by creating a new entity.
  This is most suitable for complex to-many relationships."
  [m entity-name property-name product-key]
  (let [entity-name' (name entity-name)]
    (hash-map property-name (parse-entity m entity-name')
              :PRODUCT/ID (product-key m))))

(defn parse-simple-property
  "Parse a simple to-one or to-many property that is simply a reference type.
  For example,
    {:TYPE [:VMP :DRUG_ROUTE], :VPID 318248001, :ROUTECD 26643006}
  will be parsed into:
    {:VMP/DRUG_ROUTES [:ROUTE/CD 26643006], :PRODUCT/ID 318248001}."
  [m product-key]
  (let [[file-type component-type] (:TYPE m)]
    (-> (reduce-kv (fn [m k v]
                     (when-let [[prop fk] (get-in lookup-references [file-type component-type k])] ;; turn any known reference properties into datalog references
                       (assoc m prop [fk v])))
                   {}
                   (dissoc m :TYPE))
        (assoc :PRODUCT/ID (product-key m)))))

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
         [[:VMP :VIRTUAL_PRODUCT_INGREDIENT]] (parse-extended-property m :VPI :VMP/INGREDIENTS :VPID)
         [[:VMP _]] (parse-simple-property m :VPID)
         [[:AMP :AMP]] (parse-product m :APID)
         [[:AMP :AP_INGREDIENT]] (parse-extended-property m :AP_INGREDIENT :AMP/EXCIPIENTS :APID)
         [[:AMP _]] (parse-simple-property m :APID)
         [[:VMPP :VMPP]] (parse-product m :VPPID)
         [[:VMPP :COMB_CONTENT]] (parse-simple-property m :PRNTVPPID) ;; note reference to parent is :PRNTVPPID not :VPPID
         [[:VMPP _]] (parse-simple-property m :VPPID)       ;; for all other properties, the FK to the parent is VPPID
         [[:AMPP :AMPP]] (parse-product m :APPID)
         [[:AMPP :COMB_CONTENT]] (parse-simple-property m :PRNTAPPID) ;; for COMB_CONTENT, the parent is PRNTAPPID not :APPID
         [[:AMPP _]] (parse-simple-property m :APPID)       ;; for all other properties, the FK to the parent is APPID
         [[:INGREDIENT :INGREDIENT]] (parse-lookup m)
         [[:LOOKUP _]] (parse-lookup m)
         [[:BNF _]] (parse-bnf m)
         :else (log/info "Unknown file type / component type tuple" m)))


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
  (def conn (d/create-conn "wibble.db" schema))
  (def dir "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001")

  (parse lookup-example)
  (d/transact! conn [(parse lookup-example)])

  (require '[com.eldrix.dmd.import :as dim]
           '[clojure.core.async :as a])
  (def ch (a/chan))
  (def ch (a/chan 5 (partition-all 5000)))
  (def ch (a/chan 5 (comp (map #(parse %)) (partition-all 50000))))
  (a/thread (dim/stream-dmd "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001" ch :include #{:LOOKUP :INGREDIENT :VTM :VMP :AMP}))
  (a/thread (dim/stream-dmd "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001" ch :include #{:AMP}))
  (a/thread (dim/stream-dmd "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001" ch :include #{:INGREDIENT}))
  (a/<!! ch)
  (def batch (a/<!! ch))
  (map parse batch)
  ;; this loop imports data assuming the channel includes parsing in the transducer
  (loop [batch (a/<!! ch)]
    (when batch
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
  (take 4 lookups)
  (take 4 (map parse lookups))
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

  (def x (dim/get-component dir :AMP :AMP))
  (take 5 x)
  (take 5 (map parse x))
  (parse-product x :VPID)
  (parse x)
  (parse-property x :nspace :VPI :product-key :VPID :id-key (fn [m] (str (:VPID m) "-" (:ISID m))))
  (parse (a/<!! ch))
  (d/transact! conn [(parse x)])
  (d/transact! conn [(parse (a/<!! ch))])

  ;; get all codes for a lookup
  (d/q '[:find ?code ?desc
         :where
         [?e :BASIS_OF_NAME/CD ?code]
         [?e :BASIS_OF_NAME/DESC ?desc]]
       (d/db conn))

  (time (d/q '[:find (pull ?e [:PRODUCT/ID :PRODUCT/NM :PRODUCT/VTMID {:PRODUCT/VTM [*]} {:PRODUCT/BASIS [:BASIS_OF_NAME/DESC]}])
               :where
               [?e :PRODUCT/ID 39488411000001107]]
             (d/db conn)))
  (d/q '[:find (pull ?e [:PRODUCT/ID :PRODUCT/NM {:PRODUCT/NON_AVAIL [:VIRTUAL_PRODUCT_NON_AVAIL/DESC]}])
         :where
         [?e :PRODUCT/VTMID 108537001]]
       (d/db conn))
  (d/touch (d/entity (d/db conn) (d/q '[:find ?e .
                                        :where
                                        [?e :PRODUCT/ID 319996000]]
                                      (d/db conn))))
  (d/q '[:find (pull ?vpi [*])
         :where
         [?vpi :PRODUCT ?e]
         [?e :PRODUCT/ID 319996000]]
       (d/db conn))
  (map d/touch (map #(d/entity (d/db conn) %) (d/q '[:find [?e ...]
                                                     :where
                                                     [?e :PRODUCT/VTMID 108537001]]
                                                   (d/db conn))))
  (d/q '[:find (pull ?e [*])
         :where
         [?e :INGREDIENT/ISID 391730008]]
       (d/db conn))

  (d/q '[:find (pull ?e [* {:VMP/DRUG_ROUTES       [:ROUTE/CD :ROUTE/DESC]
                            :VMP/DRUG_FORMS        [:FORM/CD :FORM/DESC]
                            :VMP/DF_IND            [:DF_INDICATOR/CD :DF_INDICATOR/DESC]
                            :VMP/CONTROL_DRUG_INFO [:CONTROL_DRUG_CATEGORY/CD :CONTROL_DRUG_CATEGORY/DESC]
                            :VMP/PRES_STAT         [:VIRTUAL_PRODUCT_PRES_STATUS/CD :VIRTUAL_PRODUCT_PRES_STATUS/DESC]
                            :VMP/INGREDIENTS       [* {:VPI/IS [*]} {:VPI/BASIS_STRNT [*]}]}])
         :where
         [?e :PRODUCT/ID 7322211000001104]]
       (d/db conn))


  ;; generate an extended AMP
  (d/q '[:find (pull ?e [* {:AMP/EXCIPIENTS [* {:AP_INGREDIENT/IS [*]}]}
                         {:AMP/AVAIL_RESTRICT [*]}
                         {:AMP/LICENSED_ROUTES [*]}
                         {:AMP/SUPP [*]}
                         {:AMP/LIC_AUTH [*]}
                         {:AMP/VP [*
                                   {:VMP/INGREDIENTS [* {:VPI/IS [*]}]}
                                   {:VMP/DRUG_ROUTES [*]}]}])
         :where
         [?e :AMP/VP [:PRODUCT/ID 7322211000001104]]]
       (d/db conn))

  ;; find all VMPs containing amoxicillin trihydrate
  (d/q '[:find (pull ?vmp [:VMP/NM])
         :where
         [?vmp :VMP/INGREDIENTS ?vpi]
         [?vpi :VPI/IS ?ingred]
         [?ingred :INGREDIENT/NM "Amoxicillin trihydrate"]]
       (d/db conn))

  (d/q '[:find ?code ?desc
         :where
         [?e :FORM/CD ?code]
         [?e :FORM/DESC ?desc]]
       (d/db conn))
  )