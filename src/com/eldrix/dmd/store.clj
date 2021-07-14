; Copyright 2021 Mark Wardle and Eldrix Ltd
;
;   Licensed under the Apache License, Version 2.0 (the "License");
;   you may not use this file except in compliance with the License.
;   You may obtain a copy of the License at
;
;       http://www.apache.org/licenses/LICENSE-2.0
;
;   Unless required by applicable law or agreed to in writing, software
;   distributed under the License is distributed on an "AS IS" BASIS,
;   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;   See the License for the specific language governing permissions and
;   limitations under the License.
;;;;
(ns com.eldrix.dmd.store
  "A simple key value store for UK dm+d data."
  (:require [clojure.core.async :as a]
            [clojure.core.match :refer [match]]
            [clojure.java.io :as io]
            [clojure.tools.logging.readable :as log]
            [taoensso.nippy :as nippy])
  (:import (org.mapdb DB Serializer DBMaker BTreeMap DataOutput2)
           (java.io FileNotFoundException Closeable)
           (org.mapdb.serializer SerializerArrayTuple GroupSerializerObjectArray)
           (java.util NavigableSet Map)))

(def NippySerializer
  (proxy [GroupSerializerObjectArray] []
    (serialize [^DataOutput2 out o]
      (nippy/freeze-to-out! out o))
    (deserialize [in _i]
      (nippy/thaw-from-in! in))))

(deftype DmdStore [^DB db
                   ^Map core
                   ^Map lookups
                   ^NavigableSet vtm-vmps
                   ^NavigableSet vmp-amps
                   ^NavigableSet vmp-vmpps
                   ^NavigableSet amp-ampps
                   ^NavigableSet vmpp-ampps
                   ^Map bnf]
  Closeable
  (close [this] (.close ^DB (.db this))))

(defn open-dmd-store
  ([^String filename] (open-dmd-store filename {}))
  ([^String filename {:keys [read-only? skip-check?] :or {read-only? true}}]
   (when (and read-only? (not (.exists (io/as-file filename))))
     (throw (FileNotFoundException. (str "file `" filename "` opened read-only but not found"))))
   (when (and (not read-only?) (.exists (io/as-file filename)))
     (throw (UnsupportedOperationException. (str "file `" filename "` exists, but not opened read-only"))))
   (let [db (.make (cond-> (-> (DBMaker/fileDB filename)
                               (.fileMmapEnableIfSupported)
                               (.closeOnJvmShutdown))
                           skip-check? (.checksumHeaderBypass)
                           read-only? (.readOnly)))
         ;; core dm+d components are stored keyed with identifier
         core (.createOrOpen (.treeMap db "core" Serializer/LONG NippySerializer))
         ;; lookup tables are stored keyed with tableName-code as a string
         lookups (.createOrOpen (.treeMap db "lookups" Serializer/STRING NippySerializer))
         ;; relationships
         vtm-vmps (.createOrOpen (.treeSet db "vtm-vmps" Serializer/LONG_ARRAY))
         vmp-amps (.createOrOpen (.treeSet db "vmp-amps" Serializer/LONG_ARRAY))
         vmp-vmpps (.createOrOpen (.treeSet db "vmp-vmpps" Serializer/LONG_ARRAY))
         amp-ampps (.createOrOpen (.treeSet db "amp-ampps" Serializer/LONG_ARRAY))
         vmpp-ampps (.createOrOpen (.treeSet db "vmpp-ampps" Serializer/LONG_ARRAY))
         ;; the dm+d supplementary file is called 'bnf' and includes 'bnf' codes as well as 'ATC' codes.
         bnf (.createOrOpen (.treeMap db "bnf" Serializer/LONG NippySerializer))]
     (->DmdStore db core lookups vtm-vmps vmp-amps vmp-vmpps amp-ampps vmpp-ampps bnf))))

(defn tuple->identifier
  [[_file-type component-type]]
  (keyword "uk.nhs.dmd" (name component-type)))

(defn put-vtm
  [^DmdStore store vtm]
  (.put ^BTreeMap (.core store) (:VTMID vtm) (update vtm :TYPE tuple->identifier)))

(defn put-vmp
  [^DmdStore store vmp]
  (.put ^BTreeMap (.core store) (:VPID vmp) (update vmp :TYPE tuple->identifier))
  (when-let [vtmid (:VTMID vmp)]
    (.add ^NavigableSet (.-vtm_vmps store) (long-array [vtmid (:VPID vmp)]))))

(defn put-amp
  [^DmdStore store amp]
  (.put ^BTreeMap (.core store) (:APID amp) (update amp :TYPE tuple->identifier))
  (when-let [vpid (:VPID amp)]
    (.add ^NavigableSet (.-vmp_amps store) (long-array [vpid (:APID amp)]))))

(defn put-vmpp
  [^DmdStore store vmpp]
  (.put ^BTreeMap (.core store) (:VPPID vmpp) (update vmpp :TYPE tuple->identifier))
  (when-let [vpid (:VPID vmpp)]
    (.add ^NavigableSet (.-vmp_vmpps store) (long-array [vpid (:VPPID vmpp)]))))

(defn put-ampp
  [^DmdStore store ampp]
  (.put ^BTreeMap (.core store) (:APPID ampp) (update ampp :TYPE tuple->identifier))
  (when-let [apid (:APID ampp)]
    (.add ^NavigableSet (.-amp_ampps store) (long-array [apid (:APPID ampp)])))
  (when-let [vppid (:VPPID ampp)]
    (.add ^NavigableSet (.-vmpp_ampps store) (long-array [vppid (:APPID ampp)]))))

(defn put-ingredient
  [^DmdStore store ingred]
  (.put ^BTreeMap (.core store) (:ISID ingred) (update ingred :TYPE tuple->identifier)))

(defn put-lookup
  [^DmdStore store lookup]
  (.put ^BTreeMap (.-lookups store) (name (:ID lookup)) (dissoc lookup :ID :TYPE)))

(defn put-bnf
  [^DmdStore store bnf]
  (when (.get ^BTreeMap (.-bnf store) (or (:VPID bnf) (:APID bnf)))
    (throw (ex-info "dm+d supplementary distribution has more than one entry for each product" bnf)))
  (.put ^BTreeMap (.-bnf store) (or (:VPID bnf) (:APID bnf)) (dissoc bnf :TYPE :VPID :APID)))

(defn put-property
  [^DmdStore store component fk]
  (let [[_ property] (:TYPE component)
        concept-id (get component fk)]
    (if-let [v (.get ^BTreeMap (.core store) concept-id)]
      (let [new-value (update v property (fn [old] ((fnil conj []) old (dissoc component fk :TYPE))))]
        (.put ^BTreeMap (.core store) concept-id new-value))
      (throw (ex-info "no existing component for id" {:id concept-id :component component})))))

(defn put
  "Store a dm+d component into the backing store.
  Each component is given a type, a tuple of the file type and the component type.
  We use that tuple to determine how to insert data into the backing store.
  Parameters:
     - store : DmdStore
     - m     : component : VTM / VMP / VMPP / AMP / AMPP "
  [^DmdStore store m]
  (match [(:TYPE m)]
         [[:VTM :VTM]] (put-vtm store m)
         [[:VMP :VMP]] (put-vmp store m)
         [[:VMP _]] (put-property store m :VPID)
         [[:AMP :AMP]] (put-amp store m)
         [[:AMP _]] (put-property store m :APID)
         [[:VMPP :VMPP]] (put-vmpp store m)
         [[:VMPP :COMB_CONTENT]] (put-property store m :PRNTVPPID) ;; note reference to parent is :PRNTVPPID not :VPPID
         [[:VMPP _]] (put-property store m :VPPID)          ;; for all other properties, the FK to the parent is VPPID
         [[:AMPP :AMPP]] (put-ampp store m)
         [[:AMPP :COMB_CONTENT]] (put-property store m :PRNTAPPID) ;; for COMB_CONTENT, the parent is PRNTAPPID not :APPID
         [[:AMPP _]] (put-property store m :APPID)          ;; for all other properties, the FK to the parent is APPID
         [[:INGREDIENT :INGREDIENT]] (put-ingredient store m)
         [[:LOOKUP _]] (put-lookup store m)
         [[:BNF _]] (put-bnf store m)
         :else (log/info "Unknown file type / component type tuple" m)))

(defn import-worker
  [store ch]
  (loop [m (a/<!! ch)]
    (when m
      (put store m)
      (recur (a/<!! ch)))))

(defn create-store
  "Create a new dm+d store from data supplied in the channel."
  [filename ch]
  (let [store (open-dmd-store filename {:read-only? false})
        cpu (.availableProcessors (Runtime/getRuntime))]
    (loop [n 0 chs []]
      (if (= n cpu)
        (a/<!! (a/merge chs))
        (recur (inc n)
               (conj chs (a/thread (import-worker store ch))))))
    (log/debug "compacting data store")
    (.compact (.getStore ^BTreeMap (.core store)))
    (log/debug "import/compaction completed")
    (.close ^DB (.db store))))

(defn fetch-product [^DmdStore store ^long id]
  (.get ^BTreeMap (.core store) id))

(defn lookup [^DmdStore store nm]
  (.get ^BTreeMap (.lookups store) (name nm)))

(defn vmps-for-vtm [^DmdStore store vtmid]
  (set (map second (map seq (.subSet ^NavigableSet (.-vtm_vmps store) (long-array [vtmid 0]) (long-array [vtmid Long/MAX_VALUE]))))))

(defn amps-for-vmp [^DmdStore store vpid]
  (set (map second (map seq (.subSet ^NavigableSet (.-vmp_amps store) (long-array [vpid 0]) (long-array [vpid Long/MAX_VALUE]))))))

(defn vmpps-for-vmp [^DmdStore store vpid]
  (set (map second (map seq (.subSet ^NavigableSet (.-vmp_vmpps store) (long-array [vpid 0]) (long-array [vpid Long/MAX_VALUE]))))))

(defn ampps-for-amp [^DmdStore store apid]
  (set (map second (map seq (.subSet ^NavigableSet (.-amp_ampps store) (long-array [apid 0]) (long-array [apid Long/MAX_VALUE]))))))

(defn ampps-for-vmpp [^DmdStore store vppid]
  (set (map second (map seq (.subSet ^NavigableSet (.-vmpp_ampps store) (long-array [vppid 0]) (long-array [vppid Long/MAX_VALUE]))))))

(defn bnf-for-product [^DmdStore store ^long product-id]
  (get ^BTreeMap (.bnf store) product-id))

(defn make-extended-vpi
  [store vpi]
  (merge
    vpi
    (dissoc (fetch-product store (:ISID vpi)) :TYPE)
    {:STRNT_NMRTR_UOM (lookup store (str "UNIT_OF_MEASURE-" (:STRNT_NMRTR_UOMCD vpi)))}
    (when (:STRNT_DNMTR_UOMCD vpi)
      {:STRNT_DNMTR_UOM (lookup store (str "UNIT_OF_MEASURE-" (:STRNT_DNMTR_UOMCD vpi)))})
    {:BASIS_STRNT (lookup store (str "BASIS_OF_STRNTH-" (:BASIS_STRNTCD vpi)))}))

(defn make-extended-api
  [store api]
  (merge
    api
    (dissoc (fetch-product store (:ISID api)) :TYPE)
    (when (:UOMCD api)
      {:UOM (lookup store (str "UNIT_OF_MEASURE-" (:UOMCD api)))})))

(defn extend-lookup-property [store code-key value-key property-prefix m]
  (when-let [k (get m code-key)]
    {value-key (lookup store (str property-prefix k))}))

(defn make-extended-vmp
  "Denormalises a VMP."
  [store vmp]
  (merge
    (-> vmp
        ;; denormalize to-one relationships; these are simple lookups
        (assoc :BASIS (lookup store (str "BASIS_OF_NAME-" (:BASISCD vmp))))
        (assoc :DF_IND (lookup store (str "DF_INDICATOR-" (:DF_INDCD vmp))))
        (assoc :PRES_STAT (lookup store (str "VIRTUAL_PRODUCT_PRES_STATUS-" (:PRES_STATCD vmp))))
        (assoc :VTM (fetch-product store (:VTMID vmp)))
        ;; denormalize to-many relationships; these might be lookups or ingredients
        (update :VIRTUAL_PRODUCT_INGREDIENT
                #(map (partial make-extended-vpi store) %))
        (update :ONT_DRUG_FORM
                #(map (fn [ont-drug-form] (lookup store (str "ONT_FORM_ROUTE-" (:FORMCD ont-drug-form)))) %))
        (update :DRUG_FORM
                #(map (fn [drug-form] (lookup store (str "FORM-" (:FORMCD drug-form)))) %))
        (update :CONTROL_DRUG_INFO #(map (fn [control-info] (lookup store (str "CONTROL_DRUG_CATEGORY-" (:CATCD control-info)))) %))
        (update :DRUG_ROUTE #(map (fn [route] (lookup store (str "ROUTE-" (:ROUTECD route)))) %)))
    (when-let [non-avail-cd (:NON_AVAILCD vmp)]
      {:NON_AVAIL (lookup store (str "VIRTUAL_PRODUCT_NON_AVAIL-" non-avail-cd))})
    (when-let [unit-dose-uomcd (:UNIT_DOSE_UOMCD vmp)]
      {:UNIT_DOSE_UOM (lookup store (str "UNIT_OF_MEASURE-" unit-dose-uomcd))})
    (when-let [udfs-uomcd (:UDFS_UOMCD vmp)]
      {:UDFS_UOM (lookup store (str "UNIT_OF_MEASURE-" udfs-uomcd))})
    (bnf-for-product store (:VPID vmp))))

(defn make-extended-amp
  "Denormalises an AMP."
  [store amp]
  (merge
    (-> amp
        (assoc :VMP (fetch-product store (:VPID amp)))
        (assoc :SUPP (lookup store (str "SUPPLIER-" (:SUPPCD amp))))
        (assoc :LIC_AUTH (lookup store (str "LICENSING_AUTHORITY-" (:LIC_AUTHCD amp))))
        (assoc :AVAIL_RESTRICT (lookup store (str "AVAILABILITY_RESTRICTION-" (:AVAIL_RESTRICTCD amp))))
        (update :LICENSED_ROUTE #(map (fn [route] (lookup store (str "ROUTE-" (:ROUTECD route)))) %))
        (update :AP_INGREDIENT #(map (partial make-extended-api store) %)))
    (when-let [lic-auth-prev (:LIC_AUTH_PREVCD amp)]
      {:LIC_AUTH_PREV (lookup store (str "LICENSING_AUTHORITY-" lic-auth-prev))})
    (when-let [lic-change-cd (:LIC_AUTHCHANGECD amp)]
      {:LIC_AUTHCHANGE (lookup store (str "LICENSING_AUTHORITY_CHANGE_REASON-" lic-change-cd))})
    (when-let [comb-prod-cd (:COMBPRODCD amp)]
      {:COMBPROD (lookup store (str "COMBINATION_PROD_IND-" comb-prod-cd))})
    (when-let [flavour-cd (:FLAVOURCD amp)]
      {:FLAVOUR (lookup store (str "FLAVOUR-" flavour-cd))})
    (bnf-for-product store (:APID amp))))

(defmulti vtms "Return identifiers for the VTMs associated with this product."
          (fn [^DmdStore _store product] (:TYPE product)))

(defmulti vmps "Returns identifiers for the VMPs associated with this product."
          (fn [^DmdStore _store product] (:TYPE product)))

(defmulti vmpps "Returns identifiers for the VMPPS associated with this product."
          (fn [^DmdStore _store product] (:TYPE product)))

(defmulti amps "Returns identifiers for the AMPs associated with this product."
          (fn [^DmdStore _store product] (:TYPE product)))

(defmulti ampps "Returns identifiers for the AMPPs associated with this product."
          (fn [^DmdStore _store product] (:TYPE product)))

(defmulti extended "Returns an extended denormalized product"
          (fn [^DmdStore _store product] (:TYPE product)))

;;
;; VTM methods
;;
(defmethod vtms :uk.nhs.dmd/VTM [_store vtm]
  #{(:VTMID vtm)})

(defmethod vmps :uk.nhs.dmd/VTM [store vtm]
  (vmps-for-vtm store (:VTMID vtm)))

(defmethod vmpps :uk.nhs.dmd/VTM [store vtm]
  (let [vpids (vmps-for-vtm store (:VTMID vtm))]
    (into #{} (mapcat (fn [vpid] (vmpps-for-vmp store vpid)) vpids))))

(defmethod amps :uk.nhs.dmd/VTM [store vtm]
  (into #{} (mapcat (fn [vmp] (amps-for-vmp store (:VPID vmp))) (map (partial fetch-product store) (vmps-for-vtm store (:VTMID vtm))))))

(defmethod ampps :uk.nhs.dmd/VTM [store vtm]
  (let [apids (amps store vtm)]
    (into #{} (mapcat (fn [apid] (ampps-for-amp store apid)) apids))))

(defmethod extended :uk.nhs.dmd/VTM [_store vtm]
  vtm)

;;
;; VMP methods
;;
(defmethod vtms :uk.nhs.dmd/VMP [_store vmp]
  #{(:VTMID vmp)})

(defmethod vmps :uk.nhs.dmd/VMP [_store vmp]
  #{(:VPID vmp)})

(defmethod vmpps :uk.nhs.dmd/VMP [store vmp]
  (vmpps-for-vmp store (:VPID vmp)))

(defmethod amps :uk.nhs.dmd/VMP [store vmp]
  (amps-for-vmp store (:VPID vmp)))

(defmethod ampps :uk.nhs.dmd/VMP [store vmp]
  (let [apids (amps store vmp)]
    (into #{} (mapcat (fn [apid] (ampps-for-amp store apid)) apids))))

(defmethod extended :uk.nhs.dmd/VMP [store vmp]
  (make-extended-vmp store vmp))

;;
;; AMP methods
;;

(defmethod vtms :uk.nhs.dmd/AMP [store amp]
  #{(:VTMID (fetch-product store (:VPID amp)))})

(defmethod vmps :uk.nhs.dmd/AMP [store amp]
  #{(:VPID amp)})

(defmethod vmpps :uk.nhs.dmd/AMP [store amp]
  (vmpps-for-vmp store (:VPID amp)))

(defmethod amps :uk.nhs.dmd/AMP [store amp]
  #{(:APID amp)})

(defmethod ampps :uk.nhs.dmd/AMP [store amp]
  (ampps-for-amp store (:APID amp)))

(defmethod extended :uk.nhs.dmd/AMP [store amp]
  (make-extended-amp store amp))

;; VMPP methods

(defmethod vmps :uk.nhs.dmd/VMPP [_store vmpp]
  #{(:VPID vmpp)})

;;
;; AMPP methods
;;

(defmethod amps :uk.nhs.dmd/AMPP [_store ampp]
  #{(:APID ampp)})

(comment
  (def store (open-dmd-store "dmd.db"))
  (.close store)
  (def vtm (fetch-product store 108537001))
  vtm
  (map (partial fetch-product store) (vmps store vtm))
  (extended store vtm)
  (->> (vmps store vtm)
       (map (partial fetch-product store))
       (map (partial extended store)))
  (vmpps store vtm)
  (extended store (fetch-product store 12797611000001109))
  (def amp (fetch-product store 39226611000001102))
  amp
  (extended store amp)
  (map :NM (map (partial fetch-product store) (ampps store vtm)))
  )
