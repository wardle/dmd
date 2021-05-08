(ns com.eldrix.dmd.graph
  "Provides a graph API around UK dm+d structures.
  "
  (:require [clojure.string :as str]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.dmd.store :as store]
            [com.wsscode.pathom3.connect.operation :as pco]
            [com.wsscode.pathom3.connect.indexes :as pci]
            [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
            [com.wsscode.pathom3.connect.runner :as pcr]
            [com.wsscode.pathom3.interface.eql :as p.eql]))

(defn record->map
  "Turn a record into a namespaced map."
  [n r]
  (if-not (map? r)
    r
    (reduce-kv (fn [m k v] (assoc m
                             (keyword n (name k))
                             (cond
                               (map? v) (record->map n v)
                               (seq? v) (map #(record->map n %) v)
                               :else v))) {} r)))

(pco/defresolver product-from-snomed-identifier
  "Returns a UK product by identifier.
  Fortunately, there is a 1:1 mapping between dm+d product codes and SNOMED CT
  identifiers. This means that as long as we output SNOMED namespaced
  identifiers, client applications can navigate seamlessly between dm+d and
  SNOMED."
  [{::keys [store]} {:info.snomed.Concept/keys [id]}]
  {::pco/output [:uk.nhs.dmd/VTMID :uk.nhs.dmd/VPID :uk.nhs.dmd/VPPID
                 :uk.nhs.dmd/APID :uk.nhs.dmd/APPID :uk.nhs.dmd/ISID
                 :uk.nhs.dmd/NM :uk.nhs.dmd/TYPE]}
  (record->map "uk.nhs.dmd" (store/fetch-product store id)))

(pco/defresolver fetch-vtm
  [{::keys [store]} {:uk.nhs.dmd/keys [VTMID]}]
  {::pco/output [:uk.nhs.dmd/TYPE :uk.nhs.dmd/NM :uk.nhs.dmd/VTMID :info.snomed.Concept/id]}
  (record->map "uk.nhs.dmd" (store/fetch-product store VTMID)))

(pco/defresolver fetch-extended-vmp
  [{::keys [store]} {:uk.nhs.dmd/keys [VPID]}]
  {::pco/output [:uk.nhs.dmd/VPID :uk.nhs.dmd/VTMID
                 {:uk.nhs.dmd/VTM [:uk.nhs.dmd/VTMID :uk.nhs.dmd/NM]}
                 :uk.nhs.dmd/NM]}
  (when-let [vmp (store/fetch-product store VPID)]
    (record->map "uk.nhs.dmd" (store/make-extended-vmp store vmp))))

(pco/defresolver fetch-extended-amp
  [{::keys [store]} {:uk.nhs.dmd/keys [APID]}]
  {::pco/output [:uk.nhs.dmd/VPID]}
  (when-let [amp (store/fetch-product store APID)]
    (record->map "uk.nhs.dmd" (store/make-extended-amp store amp))))

(pco/defresolver vmps-for-vtm
  [{::keys [store]} {:uk.nhs.dmd/keys [VTMID]}]
  {::pco/output [{:uk.nhs.dmd/VMPS [:uk.nhs.dmd/VPID]}]}
  {:uk.nhs.dmd/VMPS (->> (store/vmps-for-vtm store VTMID)
                         (map #(hash-map :uk.nhs.dmd/VPID %)))})


(def all-resolvers
  "dm+d resolvers; each expects an environment that contains
  a key :com.eldrix.dmd.graph/store representing a dm+d store."
  [product-from-snomed-identifier
   fetch-vtm
   fetch-extended-vmp
   fetch-extended-amp
   ;; these aliasesresolvers make it possible to navigate seamlessly from a dm+d
   ;; product to the SNOMED structures, including relationships and therefore
   ;; inference
   (pbir/alias-resolver :uk.nhs.dmd/VTMID :info.snomed.Concept/id)
   (pbir/alias-resolver :uk.nhs.dmd/VPID :info.snomed.Concept/id)
   (pbir/alias-resolver :uk.nhs.dmd/APID :info.snomed.Concept/id)
   (pbir/alias-resolver :uk.nhs.dmd/VPPID :info.snomed.Concept/id)
   (pbir/alias-resolver :uk.nhs.dmd/APPID :info.snomed.Concept/id)
   (pbir/alias-resolver :uk.nhs.dmd/ISID :info.snomed.Concept/id)])

(comment
  (def store (store/open-dmd-store "dmd-2021-04-12.db"))
  (record->map "uk.nhs.dmd" (store/fetch-product store 108537001)) ;; VTM
  (store/vmps-for-vtm store 108537001)
  (vmps-for-vtm {::store store} {:uk.nhs.dmd/VTMID 108537001})
  (record->map "uk.nhs.dmd" (store/fetch-product store 20478011000001105))
  (fetch-extended-vmp {::store store} {:uk.nhs.dmd/VPID 20478011000001105})
  (fetch-extended-amp {::store store} {:uk.nhs.dmd/APID 20428411000001100}))