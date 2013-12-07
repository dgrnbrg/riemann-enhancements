(ns riemann-enhancements.core
  (:import java.util.Date)
  (:require [clojure.core.async :as async]
            [narrator.query :refer (query-seq)]
            [clojure.tools.logging :as log]
            [narrator.operators :as n]
            [ring.middleware.json :refer (wrap-json-response)]
            [clojure.core.reducers :as r]
            [cheshire.core :as json]
            [ring.middleware.params :refer (wrap-params)]
            [ring.middleware.stacktrace :refer (wrap-stacktrace)]
            [ring.util.response :refer (response content-type status)]
            [ring.middleware.resource :refer (wrap-resource)]
            [clojure.string :as str]
            [compojure.core :refer (GET routes)]
            [datomic.api :as d :refer (q db)]))


;;opportunity to use partitions on host/service pairs
;;could use noHistory instead of excision for GC?
(def event-schema
  [[{:db/id #db/id [:db.part/db]
     :db/ident :service/name
     :db/valueType :db.type/string
     :db/index true
     :db/cardinality :db.cardinality/one
     :db/unique :db.unique/identity
     :db/doc "A service name"
     :db.install/_attribute :db.part/db}
    {:db/id #db/id [:db.part/db]
     :db/ident :host/name
     :db/valueType :db.type/string
     :db/index true
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one
     :db/doc "A host name"
     :db.install/_attribute :db.part/db}
    {:db/id #db/id [:db.part/db]
     :db/ident :metric/service
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "The service of a metric"
     :db.install/_attribute :db.part/db}
    {:db/id #db/id [:db.part/db]
     :db/ident :metric/host
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "The host of a metric"
     :db.install/_attribute :db.part/db}
    {:db/id #db/id [:db.part/db]
     :db/ident :metric/partition
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one
     :db/doc "A metric's partition"
     :db.install/_attribute :db.part/db}
    ;;TODO: determine if :metric/tick can ever be used effectively?
    ;;VAET index seems promising, since ticks are partitioned together
    ;;then membership check requires scanning 1 index for data, the other
    ;;for membership
    ;;perhaps partitions should count how many metrics they have, and
    ;;only pay the membership cost when they've got multiple series
    {:db/id #db/id [:db.part/db]
     :db/ident :metric/tick
     :db/valueType :db.type/ref
     :db/isComponent true
     :db/cardinality :db.cardinality/many
     :db/doc "A single metric event"
     :db.install/_attribute :db.part/db}
    {:db/id #db/id [:db.part/db]
     :db/ident :tick/value
     :db/valueType :db.type/double
     :db/cardinality :db.cardinality/one
     :db/doc "The value of a tick"
     :db.install/_attribute :db.part/db}
    {:db/id #db/id [:db.part/db]
     :db/ident :tick/time
     :db/index true
     :db/valueType :db.type/instant
     :db/cardinality :db.cardinality/one
     :db/doc "The time of a tick"
     :db.install/_attribute :db.part/db}
    {:db/id #db/id [:db.part/db]
     :db/ident :metrics/count
     :db/valueType :db.type/long
     :db/cardinality :db.cardinality/one
     :db/doc "A count of the number of metrics"
     :db.install/_attribute :db.part/db}
    {:db/id #db/id [:db.part/user]
     :db/ident :metrics/initialize
     :db/doc "Initializes metrics system"
     :db/fn #db/fn {:lang "clojure"
                    :params [db]
                    :code (let [metrics-total (d/entid db :metrics/total)]
                            (when-not metrics-total
                              [{:db/id (d/tempid :db.part/user)
                                :db/ident :metrics/total
                                :db/doc "The total number of metrics in this database"
                                :metrics/count 0}]))}}]
   [[:metrics/initialize]
    {:db/id #db/id [:db.part/user]
     :db/ident :metric/create
     :db/doc "Idempotently creates a metric given a host and service name"
     :db/fn #db/fn {:lang "clojure"
                    :params [db host-name service-name]
                    :code (let [host (:e (first (d/datoms db :avet :host/name host-name)))
                                service (:e (first (d/datoms db :avet :service/name service-name)))
                                host-id (or host (d/tempid :db.part/user))
                                service-id (or service (d/tempid :db.part/user))
                                metric (when (and host service)
                                         (ffirst (q '[:find ?m
                                                      :in $ ?h ?s
                                                      :where
                                                      [?m :metric/host ?h]
                                                      [?m :metric/service ?s]]
                                                    db host-id service-id)))
                                total-metrics-datom (first (d/datoms db :eavt (d/entid db :metrics/total) :metrics/count))
                                total-metrics (:v total-metrics-datom)
                                partition (keyword "mpart" (-> (inc total-metrics)
                                                               (Integer/toString 36)))
                                partition-id (d/tempid :db.part/db)]
                            (when (zero? (:e total-metrics-datom)) ;;datomic query "nil" 
                              (throw (ex-info ":metrics/total doesn't exist!" total-metrics-datom)))
                            (concat
                              (when-not host
                                [{:db/id host-id
                                  :host/name host-name}])
                              (when-not service
                                [{:db/id service-id
                                  :service/name service-name}])
                              (when-not metric
                                [{:db/id (:e total-metrics-datom)
                                  :metrics/count (inc total-metrics)}
                                 {:db/id partition-id
                                  :db/ident partition
                                  :db.install/_partition :db.part/db}
                                 {:db/id (d/tempid :db.part/user)
                                  :metric/host host-id
                                  :metric/partition partition-id
                                  :metric/service service-id}])))}}]])

  ;(d/create-database "datomic:mem://metrics")
  ;(d/create-database "datomic:free://localhost:4334/metrics")
  ;(def conn (d/connect "datomic:free://localhost:4334/metrics"))
(comment
  (q '[:find ?t
       :where [:metrics/total :metrics/count ?t]]
     (db conn))
  (:metric/partition (d/entity (db conn) (get-or-create-metric conn {:host "test1" :service "foo"})))
  (:metric/partition (d/entity (db conn) (get-or-create-metric conn {:host "test1" :service "fo1o"})))
  (:metric/partition (d/entity (db conn) (get-or-create-metric conn {:host "test2" :service "fo1o"})))


  (d/create-database "datomic:free://localhost:4334/metrics")
  (d/create-database "datomic:mem://metrics")
  (def conn (d/connect "datomic:mem://metrics"))

  (deref (d/transact conn (first event-schema)))
  (deref (d/transact conn (second event-schema)))

  (deref (d/transact conn [{:db/id #db/id [:db.part/db],
                            :db/ident :communities,
                            :db.install/_partition :db.part/db}]))

  (deref (d/transact conn [[:metric/insert "example.com" "cpu" 1.3 (System/currentTimeMillis)]]))

  (def after (deref (d/transact conn [{:db/id (d/tempid (quot (ffirst (q '[:find ?m
       :where
       [?m :metric/host]
       ] (db conn))) (* 1024 1024 1024 2)))
               :tick/time (java.util.Date.)
               :tick/value 2222.0
               }])))

  (println (-> (:tempids after) first second d/part))

  (clojure.pprint/pprint (q '[:find ?host ?service (count ?value)
       :with ?time
       :where
       [?h :host/name ?host]
       [?s :service/name ?service]
       [?m :metric/host ?h]
       [?m :metric/service ?s]
       [?m :metric/tick ?t]
       [?t :tick/time ?time]
       [?t :tick/value ?value]]
     (db conn)))
  )

(defn metric-q
  "Finds a metric id based on the given host and service"
  [db host service]
  (ffirst (q '[:find ?m
               :in $ ?host ?service
               :where
               [?h :host/name ?host]
               [?s :service/name ?service]
               [?m :metric/host ?h]
               [?m :metric/service ?s]]
             db host service)))

(defn transact-with-retries
  "Synchronously attempts to transact, retrying with the `retry-schedule` sleeps
   between each attempt. If the retry schedule is exhausted, the error will be logged.
   
   This should only be used with idempotent transactions, or transactions that
   can be duplicated if the write succeeds on the the server but not on the client.
   
   TODO: metrics tick lookups need to do deduplication"
  [conn tx-data retry-schedule]
  (loop [[timeout & retry-schedule] retry-schedule]
    (let [{:keys [error] :as result}
          (try
            @(d/transact conn tx-data)
            (catch RuntimeException e
              {:error e}))]
      (cond
          (and error timeout)
          (do (log/warn error "Retrying transaction for" (hash tx-data))
              (Thread/sleep timeout)
              (recur retry-schedule))
          error
          (log/error error "Failed transaction for" (hash tx-data))
          :else result))))

(defn get-or-create-metric
  "Takes an event and returns a metric for it. This will create the metric
   in Datomic if it doesn't already exist there."
  [conn {:keys [host service] :as e}]
  (when (and host service)
    (let [db (db conn)
          metric (metric-q db host service)
          {:keys [db-after]} (when-not metric
                               (transact-with-retries conn
                                                      [[:metric/create host service]]
                                                      (repeat 10000)))]
      (or metric (metric-q db-after host service)))))

(comment
  (get-or-create-metric conn {:host "example.com" :service "cpu"})

  (get-or-create-metric conn {:host "example.com" :service "lol3"})
  (get-or-create-metric conn {:host "example.com"})
  )

(def *random-delays* false)
(defmacro random-delay
  []
  (when *random-delays*
    `(try (Thread/sleep (rand-int 200))
          (catch Exception e# nil))))


(def reads (atom 0))
(def inputs (atom {}))
(def continues (atom {}))
(def enqueues (atom {}))
(def creates (atom 0))
(def idles (atom {}))

(defn input-chan-callback-generator
  [facet-fn facet->worker worker-constructor]
  (fn input-chan-callback
    [v c]
    (when v
      (swap! reads inc)
      (let [facet (facet-fn v)
          worker (get facet->worker facet)]
    #spy/t (str "input " v " on facet " facet " [" (:state worker :not-exist) "] with queue length " (count (:queue worker)) "\n")
      (if-let [{:keys [state in-chan ack-chan queue]} (get facet->worker facet)]
        ;; if the worker already exists
        (if (= state :idle)
          ;; if idle, immediately start on new work
          (do (swap! inputs update-in [facet] (fnil inc 0))
              (async/go (random-delay)  (async/>! in-chan v))
            #spy/t (str "starting work " v " onto facet " facet  "\n")
              (assoc-in facet->worker [facet :state] :busy))
          ;; otherwise, enqueue work
          (do
            #spy/t (str "enqueuing work " v " onto facet " facet "\n")
            (let [x (update-in facet->worker [facet :queue] conj v)]
              (swap! enqueues update-in [facet] (fnil inc 0))
              #spy/t (str "enqueue result onto facet " facet " gave " (seq (get-in x [facet :queue])) "\n")
              x
              )))
        ;; if there's no worker
        (let [{:keys [in-chan ack-chan]} (worker-constructor facet)]
          #spy/t (str "creating worker for facet " facet " seeded with " v "\n")
              (swap! creates inc)
          (assoc facet->worker facet {:state :busy
                                      :in-chan in-chan
                                      :ack-chan ack-chan
                                      :queue (conj clojure.lang.PersistentQueue/EMPTY v)})))))))

(defn ack-chan-callback-generator
  [facet {:keys [state in-chan ack-chan queue]} facet->worker worker-constructor]
  (fn ack-chan-callback
    [v c]
    #spy/t  (str "acking " v " on facet " facet " [" state "] with queue length " (count queue) "\n")
    (case v
      ;;Worker is ready for another message
      :ready
      (if-let [work (peek queue)]
        (do (async/go
              (swap! continues update-in [facet] (fnil inc 0))
              (async/>! in-chan work))
            #spy/t (str "continuing work " work " onto facet " facet  "\n")
            (let [x (-> facet->worker
                (assoc-in [facet :state] :busy)
                (assoc-in [facet :queue] (pop queue)))]
              #spy/t (str "dequeuing " (peek queue) " from facet " facet " gave " (seq (get-in x [facet :queue])) "\n")
              x
              ))
        (do
          (swap! idles update-in [facet] (fnil inc 0))
          (assoc-in facet->worker [facet :state] :idle)))
      ;;Worker has shut down
      :shutdown
      (if (seq queue)
        ;;After shutdown began, new work arrived, so start a new one
        (let [{:keys [in-chan ack-chan]} (worker-constructor facet)]
          (assoc facet->worker facet {:state :busy
                                      :in-chan in-chan
                                      :ack-chan ack-chan
                                      :queue queue}))
        ;;Clean up reference to old worker
        (dissoc facet->worker facet))
      (throw (ex-info "Unexpected message!" {:msg v :chan c})))))

(defn distributor
  "Takes an input channel, worker constructor (facet is argument, returns {:msg-chan :ack-chan :handle} map),
   and worker destructor (handle is argument, no return), and creates workers for each facet as they exist,
   destroying them when they quiesce."
  [input facet-fn worker-constructor]
  (async/go-loop [facet->worker {}]
                 (let [callbacks
                       (conj
                         (mapv (fn [[facet {:keys [ack-chan] :as facet-data}]]
                                 ;;ack-chan message
                                 [ack-chan (ack-chan-callback-generator facet facet-data facet->worker worker-constructor)])
                               facet->worker)
                         [input (input-chan-callback-generator facet-fn facet->worker worker-constructor)])
                       callback-index (into {} callbacks)
                       [val port] (async/alts! (map first callbacks))]
                   (recur ((get callback-index port) val port)))))

(defn basic-worker
  "Invokes (f init msg) for each recieved message. init takes on that return value next iteration"
  [f init]
  (fn [facet]
    (let [in-chan (async/chan 1024)
          shutdown-chan (async/chan 1024)
          ack-chan (async/chan 1024)]
      #_(random-delay)
      (async/go
        (async/>! ack-chan :ready)
        (async/go-loop [init init]
                       (async/alt!
                         shutdown-chan ([_] #spy/t (async/>! ack-chan :shutdown))
                         in-chan ([msg]
                                  (let [init' (f init msg)]
                                          ;(println msg)
                                          #_(random-delay)
                                          (async/>! ack-chan :ready)
                                          (recur init'))))))
      {:in-chan in-chan
       :shutdown-chan shutdown-chan
       :ack-chan ack-chan})))

(def global-counts (atom {}))

(def global-acks (atom 0))

(def facet-counter (basic-worker (fn [c x] 
                                   #spy/t (str "x is " x "\n")
                                   (swap! global-acks inc)
                                   (swap! global-counts update-in [(mod x 5)] (fnil conj []) x)
                                   nil)
                                 0))

(comment

  (require 'spyscope.repl)
  (spyscope.repl/trace-clear)
  (spyscope.repl/trace-query)

  (println @global-acks)

  (println @inputs @continues @enqueues @reads @creates @global-acks @idles
           (into {} (for [[k v] @global-counts] [k (count v)])))

  (let [input (async/chan 10000)]
    (async/onto-chan input (range 1000))
    (distributor input #(mod % 5) facet-counter))

(every? identity (for [[_ vals] @global-counts] (= vals (sort vals))))

  (println @global-counts)

  (println (count (mapcat val @global-counts)))
  (println (count @global-counts))
  (println (map count @global-counts)))

(defn log-to-datomic
  "Takes the uri of the datomic db that will be used to store the metrics. This should be the only
   thing going into that db (remember that transactors have an unlimited number of dbs)."
  [uri]
  (d/create-database uri)
  (let [conn (d/connect uri)
        log-chan (async/chan (async/dropping-buffer 5000))
        error-chan (async/chan)]

    (deref (d/transact conn (first event-schema)))
    (deref (d/transact conn (second event-schema)))

    (async/thread
      (loop [pending []
             timeout (async/timeout 10000)
             flush? false]
        (if flush?
          (do
            ;; TODO: Could fail
            (transact-with-retries conn pending (repeat 10 10000))
            ;;TODO finish here
            (recur [] (async/timeout 10000) false))
          (async/alt!!
            log-chan ([e]
                      (let [metric-id (get-or-create-metric conn e)
                            tick-id (-> (d/entity (db conn) metric-id)
                                        :metric/partition
                                        d/tempid)
                            tx (when (and metric-id (:metric e))
                                 {:db/id tick-id
                                  :metric/_tick metric-id
                                  :tick/time (java.util.Date.)
                                  :tick/value (double (:metric e))})]
                        (recur (if tx
                                 (conj pending tx)
                                 pending)
                               timeout
                               (= (count pending) 500))))
            timeout ([_]
                     (recur pending (async/timeout 10000) (boolean (seq pending))))))))
    (fn [e]
      ;;TODO: Could throw
      (async/put! log-chan e))))

(comment
  
  (q '[:find ?host ?service (count)
       :where
       [?h :host/name ?host]
       [?s :service/name ?service]
       [?m :metric/host ?h]
       [?m :metric/service ?s]]
     (db (d/connect "datomic:mem://metrics"))
     )

  (q '[:find ?host
       :where
       [_ :host/name ?host]
       ]
     (db (d/connect "datomic:mem://metrics"))
     )

  (dorun (map (fn [[t]]
         (println (d/ident (db conn) (d/part t)))
                (println (d/touch (d/entity (db conn) t)))
         
         ) (q '[:find ?t
       :in $ ?host ?service
       :where
       [?h :host/name ?host]
       [?s :service/name ?service]
       [?m :metric/host ?h]
       [?m :metric/service ?s]
       [?m :metric/tick ?t]
       ]
     (db conn) "localhost" "cpu")))
  )

;(metric-ts-q (db conn) cpu (java.util.Date. (- (System/currentTimeMillis) (* 1000 60 10))) (java.util.Date.)) 

(defn metric-ts-q
  "Finds the metric timeseries from the given metric between the requested start and end.
   
   This should be fast."
  [db metric start end]
  (let [metric-partition (->> metric
                              (d/entity db)
                              (:metric/partition)
                              (d/entid db))
        start-id (d/entid-at db metric-partition start)
        end-ms (.getTime ^Date end)
        s (->> (d/seek-datoms db :eavt start-id)
               (take-while (fn [datom]
                             (-> (:e datom)
                                 (d/part)
                                 (= metric-partition))))
               (partition-by :e)
               ;;Optimization: this filter can drop at most 1 value from the start and end
               (filter (fn [values]
                         (= 2 (count values))))
               (map (fn [[d1 d2]]
                      (hash-map
                        (d/ident db (:a d1)) (:v d1)
                        (d/ident db (:a d2)) (:v d2))))
               (take-while (fn [{t :tick/time}]
                             (>= end-ms (.getTime ^Date t)))))]
    s))

(defn or-fn
  "like `or`, but a function instead of a macro"
  [& args]
  (reduce #(or %1 %2) false args))

(def presence
  "Narrator aggregator that returns truthy if any of the values
   it is combining are truthy"
  (narrator.core/monoid-aggregator
    :initial (constantly false)
    :pre-process (constantly true)
    :combine or-fn))

(defn parse-minutes->millitime
  "Used in giraffe's requests"
  [time-str]
  (let [[_ min] (re-matches #"^-(\d+)minutes$" time-str)]
    (* (Long. min) 60 1000)))

(defn surround-interval-with-nil-values
  "Takes a start and end date and a sequence of samples
   destined for graphite, and surrounds them with nil samples."
  [^Date start ^Date end samples]
  (let [s (-> start .getTime (quot 1000))
        e (-> end .getTime (quot 1000))]
    (concat [[nil s]]
            samples
            [[nil e]])))

(defn downsample-and-preserve-meaningful-gaps
  "Takes a series of ticks from the database and downsamples
   them to the target `ms-per-interval`, if needed. Emits
   a nil-valued point in intervals for which the `meaningful-gap`
   has elapsed without seeing any samples"
  [quantile ticks ms-per-interval meaningful-gap]
  (->> (query-seq
         [:tick/value
          {:quantile [(n/quantiles {:quantiles [quantile]})
                      #(get % quantile)]
           :presence? (n/moving (max meaningful-gap  ms-per-interval)
                                presence)}]
         {:timestamp (fn [{t :tick/time}] (.getTime ^Date t))
          :period ms-per-interval}
         ticks)
       (map (fn [{:keys [timestamp value]}]
              (let [{p :presence? q :quantile} value]
                [(cond
                   q q
                   (apply or-fn p) ::remove
                   :else nil)
                 (quot timestamp 1000)])))
       (remove (fn [[v t]]
                 (= ::remove v)))))

(def temp-dashboard
  {:scheme ["#0000ff"
            "#000000"
            "#ff0000"
            "#0000ff"
            "#00ff00"
            "#000000"
            "#0000ff"
            "#00ff00"
            "#ff0000"]
   :dashboards [{:name "Demo"
                 :refresh 2500
                 :description "This is just a sample. Try feeding riemann-healh information into riemann for \"localhost\"!"
                 :metrics [{:alias "cpu"
                            :target "localhost:cpu"
                            :description "cpu usage"
                            :summary "avg"
                            :renderer "line"
                            :interpolation "linear"}]}]})

(defn graphite-api
  [dashboards uri]
  ;; By creating here, we prevent a race condition on whether this server runs first, or the
  ;; stream sink that logs to datomic
  (d/create-database uri)
  (let [conn (d/connect uri)]
    (-> (routes
          (GET "/dashboards.js" {:keys [server-port server-name]}
               (let [{scheme :scheme
                      boards :dashboards} dashboards]
                 (str "var graphite_url = 'http://" server-name ":" server-port "';\n\n"
                      "var dashboards = " (json/generate-string boards) ";\n\n"
                      "var scheme = [" (->> scheme
                                            (map #(str \' % \'))
                                            (str/join ", "))
                      "].reverse();")))
          (GET "/render" [target from until format maxDataPoints jsonp]
               (let [db (db conn)
                     [host service] (str/split target #":" 2)
                     delta (parse-minutes->millitime from)
                     metric (metric-q db host service)
                     start (Date. (- (System/currentTimeMillis) delta))
                     end (Date.)
                     ticks (metric-ts-q db metric start end)
                     ms-per-interval (max (quot delta (quot (Long. maxDataPoints) 2)) 0)
                     json (json/generate-string [
                                                 {:target (str target "-50")
                                                  :datapoints (->> (if (seq ticks)
                                                                     (downsample-and-preserve-meaningful-gaps
                                                                       0.5 ticks ms-per-interval 30000)
                                                                     [])
                                                                   (surround-interval-with-nil-values
                                                                     start end))}
                                                 #_{:target (str target "-75")
                                                    :datapoints (->> (if (seq ticks)
                                                                       (downsample-and-preserve-meaningful-gaps
                                                                         0.75 ticks ms-per-interval 30000)
                                                                       [])
                                                                     (surround-interval-with-nil-values
                                                                       start end))}
                                                 #_{:target (str target "-99")
                                                    :datapoints (->> (if (seq ticks)
                                                                       (downsample-and-preserve-meaningful-gaps
                                                                         0.99 ticks ms-per-interval 30000)
                                                                       [])
                                                                     (surround-interval-with-nil-values
                                                                       start end))}
                                                 ])]
                 #_(when (seq ticks)
                     (println "Downsample ratio:" (double (/ (count med-data) (count ticks)))
                              "total:" (count med-data)))
                 (assert (= format "json"))
                 (-> (response (str jsonp "(" json ")"))
                     (status 200)
                     (content-type "application/json")))))
        (wrap-resource "giraffe")
        (wrap-json-response)
        (wrap-params)
        (wrap-stacktrace))))
