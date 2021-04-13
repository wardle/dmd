(ns com.eldrix.dmd.core
  (:require [clojure.core.async :as a]
            [com.eldrix.dmd.import :as dim]
            [com.eldrix.dmd.store :as st]))

(defn import-dmd
  [filename dir]
  (let [ch (a/chan)]
    (a/thread (dim/stream-dmd dir ch :ordered? true))
    (st/create-store filename ch)))

(comment
  (import-dmd "dmd.db" "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001")
  (def store (st/open-dmd-store "dmd.db"))
  (.close store)
  (def vtm (st/fetch-product store 108537001))
  vtm
  (st/vmps store vtm)
  (st/vmpps store vtm)
  (st/amps store vtm)
  (map :NM (map (partial st/fetch-product store) (st/ampps store vtm)))
  )

