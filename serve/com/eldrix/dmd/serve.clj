(ns com.eldrix.dmd.serve
  "Provides a web service for dm+d data."
  (:gen-class)
  (:require [clojure.data.json :as json]
            [clojure.tools.logging.readable :as log]
            [io.pedestal.http :as http]
            [io.pedestal.http.content-negotiation :as conneg]
            [io.pedestal.http.route :as route]
            [io.pedestal.interceptor :as intc]
            [com.eldrix.dmd.store2]
            [ring.util.response :as ring-response]
            [clojure.string :as str])
  (:import (java.net URLDecoder)
           (com.fasterxml.jackson.core JsonGenerator)
           (java.time LocalDate)
           (java.time.format DateTimeFormatter)))

(defn response [status body & {:as headers}]
  {:status  status
   :body    body
   :headers headers})

(def ok (partial response 200))
(def not-found (partial response 404))

(def supported-types ["text/html" "application/edn" "application/json" "text/plain"])
(def content-neg-intc (conneg/negotiate-content supported-types))

(def formatters
  {LocalDate #(.format (DateTimeFormatter/ISO_DATE) %)})

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

(def common-interceptors [coerce-body content-neg-intc entity-render])


(defn prepare-result [m]
  (reduce-kv (fn [result k v]
               (if (or (= :db/id k) (= :LOOKUP/KIND k))
                 result
                 (assoc result k (if (map? v) (prepare-result v) v)))) {} m))

(def fetch-product
  {:name
   ::fetch-product
   :enter
   (fn [context]
     (let [store (get-in context [:request ::store])
           product-id (get-in context [:request :path-params :product-id])]
       (if-not product-id
         context
         (assoc context :result (prepare-result (com.eldrix.dmd.store2/fetch-product store (Long/parseLong product-id)))))))})

(def fetch-lookup
  {:name
   ::fetch-lookup
   :enter
   (fn [context]
     (let [store (get-in context [:request ::store])
           lookup-kind (str/upper-case  (get-in context [:request :path-params :lookup-kind]))]
       (if-not lookup-kind
         context
         (assoc context :result (map prepare-result (com.eldrix.dmd.store2/lookup store lookup-kind))))))})


(def routes
  (route/expand-routes
    #{["/dmd/v1/product/:product-id" :get (conj common-interceptors fetch-product)]
      ["/dmd/v1/product/:product-id/vtms" :get (conj common-interceptors fetch-vtms)]
      ["/dmd/v1/lookup/:lookup-kind" :get (conj common-interceptors fetch-lookup)]}))

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

(defn -main [& args]
  (if-not (= 2 (count args))
    (println "Usage: clj -M:serve <database> <port>\n    or java -jar dmd-server.jar <database> <port>")
    (let [[filename port] args
          store (com.eldrix.dmd.store2/open-store filename)
          port' (Integer/parseInt port)]
      (log/info "starting NHS dm+d server" {:port port' :filename filename})
      (start-server store port'))))

(comment
  (do
    (require '[io.pedestal.test])
    (defn test-request [verb url]
      (io.pedestal.test/response-for (::http/service-fn @server) verb url)))
  )