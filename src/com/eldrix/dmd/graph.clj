(ns com.eldrix.dmd.graph
  "Provides a graph API around UK dm+d structures."
  (:require [clojure.string :as str]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.dmd.core :as dmd]
            [com.wsscode.pathom3.connect.operation :as pco]
            [com.wsscode.pathom3.connect.indexes :as pci]
            [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
            [com.wsscode.pathom3.connect.runner :as pcr]
            [com.wsscode.pathom3.interface.eql :as p.eql]))

(defn namespace-map
  "Add the namespace to all keywords, recursing into the map."
  [n x]
  (if-not (map? x)
    x
    (reduce-kv
      (fn [m k v]
        (assoc m
          (if-not (keyword? k)
            k
            (keyword n (name k)))
          (cond
            (map? v) (namespace-map n v)
            (coll? v) (map #(namespace-map n %) v)
            :else v))) {} x)))

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
                 (namespace-map "uk.nhs.dmd" (dmd/fetch-product store id)))

(pco/defresolver fetch-vtm
                 [{::keys [store]} {:uk.nhs.dmd/keys [VTMID]}]
                 {::pco/output [:uk.nhs.dmd/VTMID
                                :uk.nhs.dmd/TYPE
                                :uk.nhs.dmd/NM
                                :uk.nhs.dmd/INVALID
                                :uk.nhs.dmd/ABBREVNM
                                :uk.nhs.dmd/VTMIDPREV
                                :uk.nhs.dmd/VTMIDDT]}
                 (namespace-map "uk.nhs.dmd" (dmd/fetch-product store VTMID)))

(def code-properties
  [:uk.nhs.dmd/CD
   :uk.nhs.dmd/CDDT
   :uk.nhs.dmd/CDPREV
   :uk.nhs.dmd/DESC])

(pco/defresolver fetch-extended-vmp
                 [{::keys [store]} {:uk.nhs.dmd/keys [VPID]}]
                 {::pco/output [:uk.nhs.dmd/VPID
                                :uk.nhs.dmd/VTMID
                                {:uk.nhs.dmd/VTM [:uk.nhs.dmd/VTMID :uk.nhs.dmd/NM]}
                                :uk.nhs.dmd/NM
                                {:uk.nhs.dmd/UNIT_DOSE_UOM code-properties}
                                {:uk.nhs.dmd/DRUG_ROUTE code-properties}
                                :uk.nhs.dmd/BNF :uk.nhs.dmd/ATC
                                {:uk.nhs.dmd/CONTROL_DRUG_INFO code-properties}
                                {:uk.nhs.dmd/ONT_DRUG_FORM code-properties}
                                :uk.nhs.dmd/VPIDPREV :uk.nhs.dmd/DDD
                                {:uk.nhs.dmd/PRES_STAT code-properties}
                                :uk.nhs.dmd/PRES_STATCD
                                {:uk.nhs.dmd/UDFS_UOM code-properties}
                                {:uk.nhs.dmd/DF_IND code-properties}
                                {:uk.nhs.dmd/BASIS code-properties}
                                {:uk.nhs.dmd/VIRTUAL_PRODUCT_INGREDIENT [:uk.nhs.dmd/ISID
                                                                         :uk.nhs.dmd/ISIDPREV
                                                                         :uk.nhs.dmd/STRNT_NMRTR_VAL
                                                                         :uk.nhs.dmd/NM
                                                                         :uk.nhs.dmd/ISIDDT
                                                                         {:uk.nhs.dmd/STRNT_NMRTR_UOM code-properties}
                                                                         {:uk.nhs.dmd/BASIS_STRNT code-properties}
                                                                         :uk.nhs.dmd/STRNT_NMRTR_UOMCD
                                                                         :uk.nhs.dmd/BASIS_STRNTCD]}
                                :uk.nhs.dmd/DDD_UOMCD
                                :uk.nhs.dmd/BASISCD
                                :uk.nhs.dmd/UNIT_DOSE_UOMCD
                                {:uk.nhs.dmd/DRUG_FORM code-properties}]}
                 (namespace-map "uk.nhs.dmd" (dmd/fetch-product store VPID)))

(pco/defresolver fetch-extended-amp
                 [{::keys [store]} {:uk.nhs.dmd/keys [APID]}]
                 {::pco/output [:uk.nhs.dmd/VPID]}
                 (namespace-map "uk.nhs.dmd" (dmd/fetch-product store APID)))

(pco/defresolver vmps-for-product
                 [{::keys [store]} {:uk.nhs.dmd/keys [ID]}]
                 {::pco/output [{:uk.nhs.dmd/VMPS [:uk.nhs.dmd/VPID]}]}
                 {:uk.nhs.dmd/VMPS (dmd/vmps-for-product store ID)})

(def all-resolvers
  "dm+d resolvers; each expects an environment that contains
  a key `:com.eldrix.dmd.graph/store` representing a dm+d store."
  [product-from-snomed-identifier
   fetch-vtm
   fetch-extended-vmp
   fetch-extended-amp
   vmps-for-product
   ;; these alias-resolvers make it possible to navigate seamlessly from a dm+d
   ;; product to the SNOMED structures, including relationships and therefore
   ;; inference
   (pbir/alias-resolver :uk.nhs.dmd/VTMID :info.snomed.Concept/id)
   (pbir/alias-resolver :uk.nhs.dmd/VPID :info.snomed.Concept/id)
   (pbir/alias-resolver :uk.nhs.dmd/APID :info.snomed.Concept/id)
   (pbir/alias-resolver :uk.nhs.dmd/VPPID :info.snomed.Concept/id)
   (pbir/alias-resolver :uk.nhs.dmd/APPID :info.snomed.Concept/id)
   (pbir/alias-resolver :uk.nhs.dmd/ISID :info.snomed.Concept/id)])


(comment
  (def store (dmd/open-store "dmd-2021-09-06.db"))
  (def registry (-> (pci/register all-resolvers)
                    (assoc ::store store)))
  (p.eql/process registry
                 [{[:uk.nhs.dmd/VTMID 108537001]
                   '[* :uk.nhs.dmd/NM]}])
  (namespace-map "uk.nhs.dmd" (dmd/fetch-product store 108537001)) ;; VTM
  (map #(namespace-map "uk.nhs.dmd" %) (dmd/vmps-for-product store 108537001))

  (p.eql/process registry
                 [{[:uk.nhs.dmd/VPID 39731911000001109]
                   [:uk.nhs.dmd/VTMID
                    :uk.nhs.dmd/NM
                    :uk.nhs.dmd/VMPS
                    :info.snomed.Concept/id]}])
  )