(ns riemann-enhancements.core
  (:import java.util.Date)
  (:require [clojure.core.async :as async]
            [narrator.query :refer (query-seq)]
            [narrator.operators :as n]
            [ring.adapter.jetty :refer (run-jetty)]
            [ring.middleware.json :refer (wrap-json-response)]
            [clojure.core.reducers :as r]
            [cheshire.core :as json]
            [ring.middleware.params :refer (wrap-params)]
            [ring.middleware.stacktrace :refer (wrap-stacktrace)]
            [ring.middleware.reload :refer (wrap-reload)]
            [ring.util.response :refer (response content-type)]
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

(defn get-or-create-metric
  "Takes an event and returns a metric for it. This will create the metric
   in Datomic if it doesn't already exist there."
  [conn {:keys [host service] :as e}]
  (when (and host service)
    (let [db (db conn)
          metric (metric-q db host service)
          {:keys [db-after]} (when-not metric
                               @(d/transact conn [[:metric/create host service]]))]
      (or metric (metric-q db-after host service)))))

(comment
  (get-or-create-metric conn {:host "example.com" :service "cpu"})

  (get-or-create-metric conn {:host "example.com" :service "lol3"})
  (get-or-create-metric conn {:host "example.com"})
  )

(defn log-to-datomic
  [uri]
  (d/create-database uri)
  (let [conn (d/connect uri)
        log-chan (async/chan)
        error-chan (async/chan)]

    (deref (d/transact conn event-schema))

    (async/thread
      (loop [pending []
             timeout (async/timeout 10000)
             flush? false]
        (if flush?
          (do
            ;; TODO: Could fail
            (try (deref (d/transact-async conn pending))
                 (catch Exception e
                   (.printStackTrace e)
                   (throw e)
                   )
                 )
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
  [& args]
  (reduce #(or %1 %2) false args))

(def presence
  (narrator.core/monoid-aggregator
    :initial (constantly false)
    :pre-process (constantly true)
    :combine or-fn))

(comment (def cpu (metric-q (db conn) "localhost" "cpu"))
         (query-seq
           [:tick/value {:quantile [(n/quantiles {:quantiles [0.5]}) #(get % 0.5)]
                         :presence? [(n/moving 30000 presence) presence]}]
           {:timestamp (fn [{t :tick/time}] (.getTime ^Date t))
            :period 3370}
           (metric-ts-q (db conn) cpu (java.util.Date. (- (System/currentTimeMillis) (* 1000 60 10))) (java.util.Date.)))
              
         (query-seq
           [:tick/value {:quantile [(n/quantiles {:quantiles [0.5]}) #(get % 0.5)]
                         :presence? [(n/moving 30000 presence) first]}]
           {:timestamp (fn [{t :tick/time}] (.getTime ^Date t))
            :period 485393}
           (metric-ts-q (db conn) cpu (java.util.Date. (- (System/currentTimeMillis) (* 1000 60 60 24 5))) (java.util.Date.)))
               
         (count (metric-ts-q (db conn) cpu (java.util.Date. (- (System/currentTimeMillis) (* 1000 60 60 24 5))) (java.util.Date.))))

;;TODO: this gives strang looking results when upsampling instead of downsampling
(defn intervalize-by
  "Takes a series of ticks and intervalizes them into groups of the requested size.
   Should not have weird aliasing artifacts, as every interval will be aligned regardless
   of the supplied ticks."
  [interval-ms ticks]
  (let [->interval (fn [tick] (quot (.getTime ^Date (:tick/time tick)) interval-ms))]
    (loop [current-interval  (->interval (first ticks))
           current-vals [(:tick/value (first ticks))]
           ticks (next ticks)
           past []]
      (if (seq ticks)
        (if (= (->interval (first ticks)) current-interval)
          (recur current-interval (conj current-vals (-> ticks first :tick/value))
                 (next ticks) past)
          ;;subtract one because we want to find adjacent intervals
          (let [gaps (- (->interval (first ticks)) current-interval 1)] 
            (when (pos? gaps) (println "got gaps:" gaps))
            (recur (->interval (first ticks))
                   [(:tick/value (first ticks))]
                   (next ticks)
                   (apply conj past {:time (* interval-ms current-interval) :values current-vals}
                          (mapv (fn [i] {:time (* interval-ms (+ current-interval i 1))
                                        :values []}) (range gaps))))))
        (conj past {:time (* interval-ms current-interval) :values current-vals})))))

(defn pad-intervals
  "Takes a sequence of intervals and a start time, and pads out the sequence
   of intervals from the start time"
  [start end interval-ms intervals]
  (let [desired-last (quot end interval-ms)
        empirical-last (quot (:timestamp (last intervals)) interval-ms)
        ;;Always have 1 fewer last gaps because we'd rather not have any data
        ;;if we haven't collected any samples from the most recent interval,
        ;;since a 0 vaue there would be misleading
        last-gaps (- desired-last empirical-last 1)
        desired-first (quot start interval-ms)
        empirical-first (quot (:timestamp (first intervals)) interval-ms)
        first-gaps (- empirical-first desired-first)]
    (println "padding" first-gaps last-gaps)
    (concat
      (mapv (fn [i]
              {:timestamp (* interval-ms (+ desired-first i))
               :value 0.0})
            (range first-gaps))
      intervals
      (mapv (fn [i]
              {:timestamp (* interval-ms (+ empirical-last i))
               :value 0.0})
            (range last-gaps)))))

(defn metric-quantile
  "Takes a sequence of values and finds the given exact quantile"
  [quantile values]
  (let [sorted (sort values)
        size (count values)
        index (long (* quantile size))]
    (if (seq values) 
      (nth sorted index)
      0.0)))

(defn parse-minutes->millitime
  [time-str]
  (let [[_ min] (re-matches #"-(\d+)minutes" time-str)]
    (* (Long. min) 60 1000)))

(defn interpolator 
 "Takes a coll of 2D points and returns 
  their linear interpolation function."
 [points]
  (let [m (into (sorted-map) points)]
    (fn [x]
      (let [[[x1 y1]] (rsubseq m <= x)
            [[x2 y2]] (subseq m > x)]
        (cond
          (not x2) 0
          (not x1) 0
          :else (+ y1 (* (- x x1) (/ (- y2 y1) (- x2 x1)))))))))

(defn ema
  [decay [[x y] & pts]]
  (loop [[[x y] & pts] pts
         x* x
         y* y]
    (let [decay' (Math/pow (- 1 decay) (- x x*))]
      (if (and x y)
        (recur pts
               x 
               (+ (* decay' y*)
                  (* (- 1 decay') y)))
        y*))))

(defn region-averager
  "Returns the average of the points w/in +-r
   of the target."
 [r points]
  (let [m (into (sorted-map) points)]
    (fn [x]
      (let [pts  (subseq m > (- x r) <= (+ x r))]
        (if (seq pts)
          (/ (apply + (mapv second pts))
             (count pts))
          0)))))

(defn region-median
  "Returns the average of the points w/in +-r
   of the target."
  [r points]
  (let [m (into (sorted-map) points)]
    (fn [x]
      (let [pts (subseq m > (- x r) <= (+ x r))]
        (if (seq pts)
          (second (nth pts (quot (count pts) 2)))
          0)))))

(defn poisson-window
  "From http://en.wikipedia.org/wiki/Window_function#Exponential_or_Poisson_window"
  [N t n]
  (let [c (double (/ (dec N) 2))]
    (Math/exp (- (/ (Math/abs (- n c)) t)))))

(defn poisson-average
  [samples min-x max-x]
  (let [N 1.0
        x-range (- max-x min-x)
        ;;60 DB attenuation after half distance
        t (/ (* N 8.69) (* 2 60))
        values
        (for [[x y] samples
              :let [x-offset (/ (- x min-x) x-range)
                    weight (poisson-window N t x-offset)]]
              [weight (* weight y)])
        total-weight (apply + (mapv first values))
        total-value (apply + (mapv second values))]
    (/ total-value total-weight)))

(defn region-poisson-averager
  "Returns the average of the points w/in +-r
   of the target."
  [r points]
  (let [m (into (sorted-map) points)]
    (fn [x]
      (let [pts  (subseq m > (- x r) <= (+ x r))]
        (if (seq pts)
          (poisson-average pts (- x r) (+ x r))
          0)))))

(defn averager
  "Takes a coll of 2D points and returns a function that
   returns their average over the given interval"
  [points]
  (let [m (into (sorted-map) points)]
    (fn [x-min x-max]
      (let [xs (filter (fn [[x y]]
                         (and (>= x x-min)
                              (<= x x-max)))
                       m)]
        (if (seq xs)
          (/ (apply + (mapv second xs))
             (count xs))
          0)))))

(defn compute-data-with-fn
  [raw f earliest maxDataPoints]
  (let [data-fn (->> raw
                     (sort-by first)
                     f)
        data (map (fn [t]
                    [(data-fn t) (quot t 1000)])
                  (range earliest (System/currentTimeMillis)
                         (/ (- (System/currentTimeMillis)
                               earliest)
                            (Long. maxDataPoints))))]
    data))

#_(->> (metric-ts-q (db conn) (metric-q (db conn) "localhost" "cpu")
                   (Date. (- (System/currentTimeMillis) (* 1000 60 10)))
                   (Date.))
     (intervalize-by 30000)
     (map (partial metric-quantile 0.5))
     (clojure.pprint/pprint)
     )

(defn surround-interval-with-nil-values
  "Takes a start and end date and a sequence of samples
   destined for graphite, and surrounds them with nil samples."
  [^Date start ^Date end samples]
  (let [s (-> start .getTime (quot 1000))
        e (-> end .getTime (quot 1000))]
    (concat [[nil s]]
            samples
            [[nil e]])))

(defn handler
  [conn]
  (GET "/render" [target from until format jsonp maxDataPoints]
       (let [db (db conn)
             [host service] (str/split target #":" 2)
             delta (parse-minutes->millitime from)
             metric (metric-q db host service)
             start (Date. (- (System/currentTimeMillis) delta))
             end (Date.)
             ticks (metric-ts-q db metric start end)
             ms-per-interval (max (quot delta (quot (Long. maxDataPoints) 2)) 0)
             ;;med-data (->> ticks
             ;;              (intervalize-by ms-per-interval)
             ;;              (pad-intervals (.getTime start) (.getTime end) ms-per-interval)
             ;;              (map (fn [{:keys [time values]}]
             ;;                     [
             ;;                      (metric-quantile 0.5 values)
             ;;                      (quot time 1000)
             ;;                      ])))
             json (json/generate-string [
                                         {:target (str target "-50")
                                          :datapoints (->> (query-seq
                                                             [:tick/value
                                                              {:quantile [(n/quantiles {:quantiles [0.5]})
                                                                          #(get % 0.5)]
                                                               :presence? (n/moving (max 30000 ms-per-interval)
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
                                                                     (= ::remove v)))
                                                           (surround-interval-with-nil-values
                                                             start end))
                                          ;:datapoints med-data
                                          }
                                         #_{:target (str target "-95")
                                          :datapoints (->> ticks
                                                           (intervalize-by ms-per-interval)
                                                           (pad-intervals (.getTime start) (.getTime end) ms-per-interval)
                                                           (map (fn [{:keys [time values]}]
                                                                  [
                                                                   (metric-quantile 0.95 values)
                                                                   (quot time 1000)
                                                                   ])))}
                                         #_{:target (str target "-99")
                                          :datapoints (->> ticks
                                                           (intervalize-by ms-per-interval)
                                                           (pad-intervals (.getTime start) (.getTime end) ms-per-interval)
                                                           (map (fn [{:keys [time values]}]
                                                                  [
                                                                   (metric-quantile 0.99 values)
                                                                   (quot time 1000)
                                                                   ])))}])]
         ;(println json)
         #_(when (seq ticks)
           (println "Downsample ratio:" (double (/ (count med-data) (count ticks)))
                    "total:" (count med-data)))
         (assert (= format "json"))
         (-> (response (str jsonp "(" json ")"))
             (content-type "application/json")))))

(def myapp
  (let [conn (d/connect "datomic:free://localhost:4334/metrics")]
    (-> (handler conn)
        (wrap-json-response)
        (wrap-params)
        ((fn [h]
          (fn [r]
            (let [x (h r)]
              ;(println "headers:" (:headers x))
              ;(pr "got response" (:body x))
              x))))
        (wrap-reload)
        (wrap-stacktrace)
        )))

(comment
  (do
    (.stop myserver)
    (def myserver
      (start-server "datomic:mem://metrics" 8000))))

(defn start-server
  [uri port]
  (run-jetty  #'myapp {:port port :join? false}))
