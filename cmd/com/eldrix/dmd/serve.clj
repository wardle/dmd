(ns com.eldrix.dmd.serve
  "Provides a web service for dm+d data."
  (:gen-class)
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [com.eldrix.dmd.core :as dmd]
            [io.pedestal.http :as http]
            [io.pedestal.http.content-negotiation :as conneg]
            [io.pedestal.http.route :as route]
            [io.pedestal.interceptor :as intc]
            [io.pedestal.interceptor.error :as intc-err])
  (:import (java.time LocalDate)
           (java.time.format DateTimeFormatter)))

(defn response [status body & {:as headers}]
  {:status  status
   :body    body
   :headers headers})

(def ok (partial response 200))
(def not-found (partial response 404))

(def supported-types ["application/json" "application/edn"])
(def content-neg-intc (conneg/negotiate-content supported-types))

(def formatters
  {LocalDate #(.format (DateTimeFormatter/ISO_DATE) %)})

(def service-error-handler
  (intc-err/error-dispatch
   [context err]
   [{:exception-type :java.lang.NumberFormatException :interceptor ::fetch-product}]
   (assoc context :response {:status 400
                             :body   (str "Invalid parameters; invalid number: " (ex-message (:exception (ex-data err))))})

   :else
   (assoc context :io.pedestal.interceptor.chain/error err)))

(defn transform-content
  [body content-type]
  (when body
    (case content-type
      "text/html" body
      "text/plain" body
      "application/edn" (pr-str body)
      "application/json" (json/write-str body
                                         :value-fn (fn [k v]
                                                     (if-let [formatter (get formatters (type v))]
                                                       (formatter v)
                                                       v))))))

(defn accepted-type
  [context]
  (get-in context [:request :accept :field] "application/json"))

(defn coerce-to
  [response content-type]
  (-> response
      (update :body transform-content content-type)
      (assoc-in [:headers "Content-Type"] content-type)))

(def coerce-body
  {:name ::coerce-body
   :leave
   (fn [context]
     (if (get-in context [:response :headers "Content-Type"])
       context
       (update-in context [:response] coerce-to (accepted-type context))))})

(def entity-render
  "Interceptor to render an entity '(:result context)' into the response."
  {:name :entity-render
   :leave
   (fn [context]
     (if-let [item (:result context)]
       (assoc context :response (ok item))
       context))})

(def hidden-properties
  "A set of properties to be removed from the external API."
  #{:db/id
    :LOOKUP/KIND})

(defn ^:private hide-internals* [x]
  (cond
    (map? x)
    (reduce-kv
     (fn [result k v]
       (if (hidden-properties k)
         result
         (assoc result k (hide-internals* v)))) {} x)
    (coll? x)
    (map hide-internals* x)
    :else x))

(def hide-internals
  "Interceptor to filter an entity of internal properties, such as :db/id
  and :LOOKUP/KIND."
  {:name  :filter-internals
   :leave (fn [context]
            (if-let [result (:result context)]
              (assoc context :result (hide-internals* result))
              context))})

(def fetch-product
  {:name
   ::fetch-product
   :enter
   (fn [context]
     (let [store (get-in context [:request ::store])
           product-id (get-in context [:request :path-params :product-id])]
       (if-not product-id
         context
         (assoc context :result (dmd/fetch-product store (Long/parseLong product-id))))))})

(def search-product
  {:name
   ::search-product
   :enter
   (fn [context]
     (let [store (get-in context [:request ::store])
           s (get-in context [:request :query-params :s])]
       (if-not s
         context
         (assoc context :result (dmd/fetch-product-by-exact-name store s)))))})

(def fetch-lookup
  {:name
   ::fetch-lookup
   :enter
   (fn [context]
     (let [store (get-in context [:request ::store])
           lookup-kind (get-in context [:request :path-params :lookup-kind])]
       (if (str/blank? lookup-kind)
         context
         (assoc context :result (dmd/fetch-lookup store (str/upper-case lookup-kind))))))})

(def atc->vmps
  {:name  ::atc->vmps
   :enter (fn [context]
            (let [store (get-in context [:request ::store])
                  atc (get-in context [:request :path-params :atc])]
              (if (str/blank? atc)
                context
                (assoc context :result (dmd/vmps-from-atc store atc)))))})

(def atc->products
  {:name  ::atc->products
   :enter (fn [context]
            (let [store (get-in context [:request ::store])
                  atc (get-in context [:request :path-params :atc])]
              (if (str/blank? atc)
                context
                (assoc context :result (dmd/products-from-atc store atc)))))})

(def atc->ecl
  {:name  ::atc->ecl
   :enter (fn [context]
            (let [store (get-in context [:request ::store])
                  atc (get-in context [:request :path-params :atc])]
              (if (str/blank? atc)
                context
                (assoc context :result {:atc atc
                                        :ecl (dmd/atc->snomed-ecl store atc)}))))})

(def fetch-product-vtms
  {:name
   ::fetch-product-vtms
   :enter
   (fn [context]
     (let [store (get-in context [:request ::store])
           product-id (get-in context [:request :path-params :product-id])]
       (if-not product-id
         context
         (assoc context :result (dmd/vtms-for-product store (Long/parseLong product-id))))))})

(def fetch-product-vtm
  {:name
   ::fetch-product-vtm
   :enter
   (fn [context]
     (let [store (get-in context [:request ::store])
           product-id (get-in context [:request :path-params :product-id])]
       (if-not product-id
         context
         (assoc context :result (first (dmd/vtms-for-product store (Long/parseLong product-id)))))))})

(def fetch-product-vmps
  {:name
   ::fetch-product-vmps
   :enter
   (fn [context]
     (let [store (get-in context [:request ::store])
           product-id (get-in context [:request :path-params :product-id])]
       (if-not product-id
         context
         (assoc context :result (dmd/vmps-for-product store (Long/parseLong product-id))))))})

(def fetch-product-amps
  {:name
   ::fetch-product-amps
   :enter
   (fn [context]
     (let [store (get-in context [:request ::store])
           product-id (get-in context [:request :path-params :product-id])]
       (if-not product-id
         context
         (assoc context :result (dmd/amps-for-product store (Long/parseLong product-id))))))})

(def fetch-product-atc
  {:name
   ::fetch-product-atc
   :enter
   (fn [context]
     (let [store (get-in context [:request ::store])
           product-id (get-in context [:request :path-params :product-id])]
       (if-not product-id
         context
         (assoc context :result (dmd/atc-for-product store (Long/parseLong product-id))))))})

(def common-interceptors [service-error-handler coerce-body content-neg-intc entity-render hide-internals])

(def routes
  (route/expand-routes
   #{["/dmd/v1/product/:product-id/vtms" :get (conj common-interceptors fetch-product-vtms)]
     ["/dmd/v1/product/:product-id/vtm" :get (conj common-interceptors fetch-product-vtm)]
     ["/dmd/v1/product/:product-id/vmps" :get (conj common-interceptors fetch-product-vmps)]
     ["/dmd/v1/product/:product-id/amps" :get (conj common-interceptors fetch-product-amps)]
     ["/dmd/v1/product/:product-id/atc" :get (conj common-interceptors fetch-product-atc)]
     ["/dmd/v1/product/:product-id" :get (conj common-interceptors fetch-product)]
     ["/dmd/v1/search" :get (conj common-interceptors search-product)]
     ["/dmd/v1/lookup/:lookup-kind" :get (conj common-interceptors fetch-lookup)]
     ["/dmd/v1/atc/:atc/vmps" :get (conj common-interceptors atc->vmps)]
     ["/dmd/v1/atc/:atc/products" :get (conj common-interceptors atc->products)]
     ["/dmd/v1/atc/:atc/ecl" :get (conj common-interceptors atc->ecl)]}))

(defn inject-store
  "A simple interceptor to inject dm+d store 'store' into the context."
  [store]
  {:name  ::inject-store
   :enter (fn [context] (update context :request assoc ::store store))})

(def service-map
  {::http/routes routes
   ::http/type   :jetty
   ::http/port   8082})

(defn make-service-map [store port join?]
  (-> service-map
      (assoc ::http/port port)
      (assoc ::http/join? join?)
      (http/default-interceptors)
      (update ::http/interceptors conj (intc/interceptor (inject-store store)))))

(defn start-server
  ([store port] (start-server store port true))
  ([store port join?]
   (http/start (http/create-server (make-service-map store port join?)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; For interactive development
(defonce server (atom nil))

(defn start-dev [svc port]
  (reset! server
          (http/start (http/create-server (make-service-map svc port false)))))

(defn stop-dev []
  (http/stop @server))

(defn restart [svc port]
  (stop-dev)
  (start-dev svc port))

(comment
  (do
    (require '[io.pedestal.test])
    (defn test-request [verb url]
      (io.pedestal.test/response-for (::http/service-fn @server) verb url))))
