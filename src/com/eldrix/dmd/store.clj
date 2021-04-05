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
            [clojure.java.io :as io]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.dmd.import :as dim]
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
                   ^NavigableSet vmpp-ampps]
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
         vtm-vmps (.createOrOpen (.treeSet db "vtm-vmps" Serializer/LONG_ARRAY))
         vmp-amps (.createOrOpen (.treeSet db "vmp-amps" Serializer/LONG_ARRAY))
         vmp-vmpps (.createOrOpen (.treeSet db "vmp-vmpps" Serializer/LONG_ARRAY))
         amp-ampps (.createOrOpen (.treeSet db "amp-ampps" Serializer/LONG_ARRAY))
         vmpp-ampps (.createOrOpen (.treeSet db "vmpp-ampps" Serializer/LONG_ARRAY))]
     (->DmdStore db core lookups vtm-vmps vmp-amps vmp-vmpps amp-ampps vmpp-ampps))))

;; core dm+d components
(derive :uk.nhs.dmd/VTM :uk.nhs.dmd/COMPONENT)
(derive :uk.nhs.dmd/VMP :uk.nhs.dmd/COMPONENT)
(derive :uk.nhs.dmd/AMP :uk.nhs.dmd/COMPONENT)
(derive :uk.nhs.dmd/AMPP :uk.nhs.dmd/COMPONENT)
(derive :uk.nhs.dmd/VMPP :uk.nhs.dmd/COMPONENT)

(defmulti put
          "Store a dm+d component into the backing store.
          Parameters:
          - store : DmdStore
          - m     : component / VTM / VMP / VMPP / AMP / AMPP / TF / TFG"
          (fn [^DmdStore _store m] (:TYPE m)))

(defmethod put :uk.nhs.dmd/VTM
  [^DmdStore store component]
  (.put ^BTreeMap (.core store) (:ID component) component))

(defmethod put :uk.nhs.dmd/VMP
  [^DmdStore store vmp]
  (.put ^BTreeMap (.core store) (:ID vmp) vmp)
  (when-let [vtmid (:VTMID vmp)]
    (.add ^NavigableSet (.-vtm_vmps store) (long-array [vtmid (:ID vmp)]))))

(defmethod put :uk.nhs.dmd/AMP
  [^DmdStore store amp]
  (.put ^BTreeMap (.core store) (:ID amp) amp)
  (when-let [vpid (:VPID amp)]
    (.add ^NavigableSet (.-vmp_amps store) (long-array [vpid (:ID amp)]))))

(defmethod put :uk.nhs.dmd/VMPP
  [^DmdStore store vmpp]
  (.put ^BTreeMap (.core store) (:ID vmpp) vmpp)
  (when-let [vpid (:VPID vmpp)]
    (.add ^NavigableSet (.-vmp_vmpps store) (long-array [vpid (:ID vmpp)]))))

(defmethod put :uk.nhs.dmd/AMPP
  [^DmdStore store ampp]
  (.put ^BTreeMap (.core store) (:ID ampp) ampp)
  (when-let [apid (:APID ampp)]
    (.add ^NavigableSet (.-amp_ampps store) (long-array [apid (:ID ampp)])))
  (when-let [vppid (:VPPID ampp)]
    (.add ^NavigableSet (.-vmpp_ampps store) (long-array [vppid (:ID ampp)]))))

(defmethod put :uk.nhs.dmd/COMPONENT
  [^DmdStore store component]
  (.put ^BTreeMap (.core store) (:ID component) component))

(defmethod put :uk.nhs.dmd/INGREDIENT
  [^DmdStore store component]
  (.put ^BTreeMap (.core store) (:ID component) component))

(defmethod put :uk.nhs.dmd/LOOKUP
  [^DmdStore store lookup]
  (.put ^BTreeMap (.-lookups store) (name (:ID lookup)) (dissoc lookup :ID :TYPE)))

(defmethod put :default
  [^DmdStore store component]
  (let [[concept-id property] (:ID component)]
    (if-let [v (.get ^BTreeMap (.core store) concept-id)]
      (let [new-value (update v property (fn [old] ((fnil conj []) old (dissoc component :ID :TYPE))))]
        (.put ^BTreeMap (.core store) concept-id new-value))
      (throw (ex-info "no existing component for id" {:id concept-id :component component})))))

(defn import-dmd [filename dir]
  (let [ch (a/chan)
        store (open-dmd-store filename {:read-only? false})]
    (a/thread (dim/import-dmd dir ch {:ordered? false}))
    (loop [m (a/<!! ch)]
      (when m
        (try (put store m)
             (catch Exception e (throw (ex-info "failed to import" {:e e :component m}))))
        (recur (a/<!! ch))))
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

(defn make-extended-vpi
  [store vpi]
  (merge
    vpi
    (fetch-product store (:ISID vpi))
    {:STRNT_NMRTR_UOM (lookup store (str "UNIT_OF_MEASURE-" (:STRNT_NMRTR_UOMCD vpi)))}
    (when (:STRNT_DNMTR_UOMCD vpi)
      {:STRNT_DNMTR_UOM (lookup store (str "UNIT_OF_MEASURE-" (:STRNT_DNMTR_UOMCD vpi)))})
    {:BASIS_STRNT (lookup store (str "BASIS_OF_STRNTH-" (:BASIS_STRNTCD vpi)))}))

(defn make-extended-vmp
  "Denormalises a VMP."
  [store vmp]
  (-> vmp
      ;; denormalize to-one relationships; these are simple lookups
      (assoc :BASIS (lookup store (str "BASIS_OF_NAME-" (:BASISCD vmp))))
      (assoc :UNIT_DOSE_UOM (lookup store (str "UNIT_OF_MEASURE-" (:UNIT_DOSE_UOMCD vmp))))
      (assoc :UDFS_UOM (lookup store (str "UNIT_OF_MEASURE-" (:UDFS_UOMCD vmp))))
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
      (update :DRUG_ROUTE #(map (fn [route] (lookup store (str "ROUTE-" (:ROUTECD route)))) %))))

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

(defmethod amps :uk.nhs.dmd/VTM [store vtm]
  (into #{} (mapcat (fn [vmp] (amps-for-vmp store (:ID vmp))) (map (partial fetch-product store) (vmps-for-vtm store (:ID vtm))))))

(defmethod extended :uk.nhs.dmd/VTM [_store vtm]
  vtm)

;;
;; VMP methods
;;
(defmethod vtms :uk.nhs.dmd/VMP [_store vmp]
  #{(:VTMID vmp)})

(defmethod vmpps :uk.nhs.dmd/VMP [store vmp]
  (vmpps-for-vmp store (:VPID vmp)))

(defmethod amps :uk.nhs.dmd/VMP [store vmp]
  (amps-for-vmp store (:VPID vmp)))

(defmethod extended :uk.nhs.dmd/VMP [store vmp]
  (make-extended-vmp store vmp))
;;
;; AMP methods
;;

(defmethod vtms :uk.nhs.dmd/AMP [store amp]
  #{(:VTMID (fetch-product store (:VPID amp)))})

(defmethod vmps :uk.nhs.dmd/AMP [store amp]
  #{(:VPID amp)})

(defmethod ampps :uk.nhs.dmd/AMP [store amp]
  (ampps-for-amp store (:APID amp)))

(comment
  (import-dmd "dmd.db" "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001")
  (def store (open-dmd-store "dmd.db"))
  (.close store)
  (def vtm (fetch-product store 108537001))
  (product-type store 108537001)
  (vmps store vtm)
  (map :NM (map (partial fetch-product store) (vmps store vtm)))


  (def vmp (fetch-product store 429828006))
  (map :DESC (map (partial fetch-product store) (amps store vmp)))
  (map (partial fetch-product store) (vmpps store vmp))

  (map (partial fetch-product store) (vmps-for-vtm store (:ID vtm)))
  (def amp (fetch-product store 38259611000001106))
  (map seq (.subSet (.-vmp_amps store) (long-array [0 38259611000001106]) (long-array [Long/MAX_VALUE 38259611000001106])))
  (vtms store amp)
  (ampps store (fetch-product store 38259611000001106))

  (amps store vtm)
  (vmps-for-vtm store vtm)
  (map (partial fetch-product store) (vmps-for-vtm store vtm))
  (.-vtm_vmps store)
  (fetch-product store 39233511000001107)
  (vtms store (fetch-product store 39233511000001107))
  (first (map (partial fetch-product store) (vmps-for-vtm store 108537001)))
  (map (partial amps store) (map (partial fetch-product store) (vmps-for-vtm store 108537001)))

  (fetch-product store 29932711000001105)

  (make-extended-vmp store (fetch-product store 319283006))
  (make-extended-vmp store (fetch-product store 39233511000001107))
  (lookup store :VIRTUAL_PRODUCT_NON_AVAIL-0001)
  (.lookups store)
  (.get (.core store) 39211611000001104)
  (.get (.core store) 39233511000001107)
  (.get (.core store) 714080005)
  (.get (.lookups store) (name :VIRTUAL_PRODUCT_NON_AVAIL-0001))
  (.get (.lookups store) (name :CONTROL_DRUG_CATEGORY-0000))
  (.get (.lookups store) (name :ONT_FORM_ROUTE-0022))
  (.lookups store)
  )