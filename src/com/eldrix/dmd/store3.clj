(ns com.eldrix.dmd.store3
  "Experimental store using xtdb."
  (:require [clojure.core.async :as a]
            [com.eldrix.dmd.parse :as dp]
            [xtdb.api :as xt]))


(defn make-id
  "Given data in a map, return something that can be used as an identifier."
  [m]
  (let [k (or (when (:PRODUCT/ID m) :PRODUCT/ID)
              (when (:INGREDIENT/ISID m) :INGREDIENT/ISID)
              (first (filter #(= "CD" (name %)) (keys m)))
              (throw (ex-info "Unable to determine id" m)))]
    (select-keys m [k])))

(comment
  (System/getProperty "os.arch")
  (def node (xt/start-node {}))
  (xt/status node)
  (xt/submit-tx node [[::xt/put {:xt/id :manifest
                                 :name  "Mark"}]])
  (xt/entity (xt/db node) :manifest)
  (def ch (a/chan 1 (comp (map dp/parse) (map #(assoc % :xt/id (make-id %))) (partition-all 1000))))
  (a/close! ch)
  (require '[com.eldrix.dmd.import])
  (a/thread (com.eldrix.dmd.import/stream-dmd "/Users/mark/Dev/trud/cache" ch))
  (def batch (a/<!! ch))
  batch
  (vec (for [item batch] [::xt/put item]))
  (xt/submit-tx node (vec (for [item batch] [::xt/put item])))
  batch
  (xt/q (xt/db node)
        '{:find  [(pull ?lookup [*])]
          :where [[?lookup :UNIT_OF_MEASURE/CD 10697611000001108]]})
  (xt/q (xt/db node)
        '{:find  [?desc]
          :where [[?e :SUPPLIER/CD 2068801000001106]
                  [?e :SUPPLIER/DESC ?desc]]})
  (xt/q (xt/db node)
        '{:find [(pull ?e [*])]
          :where [[?e :PRODUCT/ID 777858001]]})
  (xt/q (xt/db node)
        '{:find [(pull ?e [*])]
          :where [[?e :PRODUCT/ID 41070911000001109]]})
  (xt/submit-tx node [[::xt/put (a/<!! ch)]]))



