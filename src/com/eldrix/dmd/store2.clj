(ns com.eldrix.dmd.store2
  "A datalog store for dm+d"
  (:require [clojure.core.match :refer [match]]
            [clojure.tools.logging.readable :as log]
            [datalevin.core :as d]
            [com.eldrix.dmd.import :as dim]))


(def schema
  "The datalog-based schema is a close representation to the source dm+d data
  structures. The differences are:
   - properties are namespaced; all dm+d products (e.g. VTM/VMP etc) use
   :PRODUCT while other entities use a code representing the filename from which
   they were derived (e.g. :LOOKUP)
   - lookups are referenced by their code (e.g. :BASIS/CD \"0003\") and
   the source entity has a property :PRODUCT/BASIS.
   - the source entity does not have a property :PRODUCT/BASISCD but instead
   the code could be looked up using {:PRODUCT/BASIS [:BASIS/CD]}"
  {:PRODUCT/ID                     {:db/unique :db.unique/identity}
   :NAMECHANGE_REASON/CD           {:db/unique :db.unique/identity}
   :PRODUCT/NAMECHANGE_REASON      {:db/valueType :db.type/ref}
   :BASIS_OF_NAME/CD               {:db/unique :db.unique/identity}
   :PRODUCT/BASIS                  {:db/valueType :db.type/ref}
   :VIRTUAL_PRODUCT_PRES_STATUS/CD {:db/unique :db.unique/identity}
   :PRODUCT/PRES_STAT              {:db/valueType :db.type/ref}
   :DF_INDICATOR/CD                {:db/unique :db.unique/identity}
   :PRODUCT/DF_IND                 {:db/valueType :db.type/ref}
   :PRODUCT/VTM                    {:db/valueType :db.type/ref}})

(defn parse-product [m id-key]
  (let [[file-type component-type] (:TYPE m)]
    (reduce-kv (fn [m k v]
                 (assoc m (keyword "PRODUCT" (name k)) v))
               {:PRODUCT/TYPE component-type
                :PRODUCT/ID   (get m id-key)}
               (dissoc m id-key :TYPE))))

(defn parse-vtm [m]
  )
(defn parse-amp [m]
  )
(defn parse-vmp [m]
  (-> (cond-> (parse-product m :VPID)
              (:BASISCD m) (assoc :PRODUCT/BASIS [:BASIS_OF_NAME/CD (:BASISCD m)])
              (:BASIC_PREVCD m) (assoc :PRODUCT/BASIS_PREV [:BASIS_OF_NAME/CD (:BASIS_PREVCD m)])
              (:PRES_STATCD m) (assoc :PRODUCT/PRES_STAT [:VIRTUAL_PRODUCT_PRES_STATUS/CD (:PRES_STATCD m)])
              (:VPIDPREV m) (assoc :PRODUCT/PREV [:PRODUCT/ID (:VPIDPREV m)])
              (:DF_INDCD m) (assoc :PRODUCT/DF_IND [:DF_INDICATOR/CD (:DF_INDCD m)])
              (:VTMID m) (assoc :PRODUCT/VTM [:PRODUCT/ID (:VTMID m)]))
      (dissoc :PRODUCT/BASISCD
              :PRODUCT/PRES_STATCD
              :PRODUCT/DF_INDCD)))
(defn parse-ampp [m]
  )
(defn parse-vmpp [m]
  )
(defn parse-lookup [m]
  (let [[file-type component-type] (:TYPE m)
        nspace (name component-type)
        m' (dissoc m :TYPE :ID)]
    (-> (reduce-kv (fn [m k v] (assoc m (keyword nspace (name k)) (or v ""))) {} m'))))

(defn parse-ingredient [m]
  )
(defn parse-bnf [m]
  )
(defn parse-property [m fk]
  )

(defn parse
  "Parse a dm+d component into a map suitable for storing in the datalog store.
  Each component is given a type, a tuple of the file type and the component type.
  We use that tuple to determine how to parse data.
  Parameters:
     - m : dm+d component with :TYPE property a tuple of file and component type."
  [m]
  (match [(:TYPE m)]
         [[:VTM :VTM]] (parse-product m :VTMID)
         [[:VMP :VMP]] (parse-vmp m)
         [[:VMP _]] (parse-property m :VPID)
         [[:AMP :AMP]] (parse-amp m)
         [[:AMP _]] (parse-property m :APID)
         [[:VMPP :VMPP]] (parse-vmpp m)
         [[:VMPP :COMB_CONTENT]] (parse-property m :PRNTVPPID) ;; note reference to parent is :PRNTVPPID not :VPPID
         [[:VMPP _]] (parse-property m :VPPID)              ;; for all other properties, the FK to the parent is VPPID
         [[:AMPP :AMPP]] (parse-ampp m)
         [[:AMPP :COMB_CONTENT]] (parse-property m :PRNTAPPID) ;; for COMB_CONTENT, the parent is PRNTAPPID not :APPID
         [[:AMPP _]] (parse-property m :APPID)              ;; for all other properties, the FK to the parent is APPID
         [[:INGREDIENT :INGREDIENT]] (parse-ingredient m)
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
  (parse lookup-example)
  (d/transact! conn [(parse lookup-example)])
  (require '[com.eldrix.dmd.import :as dim]
           '[clojure.core.async :as a])
  (def ch (a/chan))
  (def ch (a/chan 5 (partition-all 500)))
  (a/thread (dim/stream-dmd "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001" ch :include #{:LOOKUP :VTM :VMP}))
  (a/thread (dim/stream-dmd "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001" ch :include #{:VMP}))
  (def x (a/<!! ch))
  (print x)
  (parse x)
  (parse (a/<!! ch))
  (d/transact! conn [(parse x)])
  (d/transact! conn [(parse (a/<!! ch))])
  (loop [batch (a/<!! ch)]
    (when batch
      (println "processing batch")
      (d/transact! conn (map parse batch))
      (recur (a/<!! ch))))
  (d/transact! conn [{:ID 24700007 :NAMECHANGE_REASON [:NAMECHANGE_REASON/CD "0003"] :DESC "sausages"}])
  (d/q '[:find ?code ?desc
         :where
         [?e :BASIS_OF_NAME/CD ?code]
         [?e :BASIS_OF_NAME/DESC ?desc]]
       (d/db conn))
  (time (d/q '[:find (pull ?e [:PRODUCT/ID :PRODUCT/NM :PRODUCT/VTMID {:PRODUCT/VTM [:PRODUCT/NM]} {:PRODUCT/BASIS [:BASIS_OF_NAME/DESC]}])
               :where
               [?e :PRODUCT/ID 39488411000001107]]
             (d/db conn)))
  (d/q '[:find (pull ?e [*])
         :where
         [?e :PRODUCT/ID 39488411000001107]]
       (d/db conn))
  )