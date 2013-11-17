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
  [{:db/id #db/id [:db.part/db]
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
   {:db/id #db/id [:db.part/user]
    :db/ident :metric/insert
    :db/fn #db/fn {:lang "clojure"
                   :params [db host-name service-name value time]
                   ;;In the code, *-id is either an entity id or a tempid, whereas
                   ;;the plain * is either an entity id or nil. Thus, the pattern
                   ;;is that we check if the entity exists, and if so, conditionally
                   ;;do some lookup/transaction expansion
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
                               metric-id (or metric (d/tempid :db.part/user))
                               tick-id (d/tempid :db.part/user)]
                           (concat
                             (when-not host
                               [{:db/id host-id
                                 :host/name host-name}])
                             (when-not service
                               [{:db/id service-id
                                 :service/name service-name}])
                             (if metric
                               [{:db/id metric-id
                                 :metric/tick tick-id}
                                {:db/id tick-id
                                 :tick/time (java.util.Date. time)
                                 :tick/value value}]        
                               [{:db/id metric-id
                                 :metric/host host-id
                                 :metric/service service-id
                                 :metric/tick tick-id}
                                {:db/id tick-id
                                 :tick/time (java.util.Date. time)
                                 :tick/value value}]))
                           )}}])

  ;(d/create-database "datomic:mem://metrics")
  ;(d/create-database "datomic:free://localhost:4334/metrics")
  ;(def conn (d/connect "datomic:free://localhost:4334/metrics"))
(comment
  (d/create-database "datomic:mem://metrics")
  (def conn (d/connect "datomic:mem://metrics"))

  (deref (d/transact conn event-schema))

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

(defn get-or-create
  "Gets the unique entity for a given AV pair. Returns either
   the real id+nil, or a tempid+txn to be included with the actual query."
  [db a v]
  (let [old-id (ffirst (q '[:find ?e
                            :in $ ?a ?v
                            :where [?e ?a ?v]]
                          db a v))
        id (or old-id (d/tempid :db.part/user))
        txn (when-not old-id
              [[:db/add id a v]])]
    [id txn]))

(defn get-or-create-metric
  "Takes an event and returns a metric for it. This will create the metric
   in Datomic if it doesn't already exist there.
   
   TODO: make this atomic/idempotent"
  [conn {:keys [host service] :as e}]
  (when (and host service)
    (let [db (db conn)
          in-db (ffirst (q '[:find ?m
                             :in $ ?host ?service
                             :where
                             [?h :host/name ?host]
                             [?s :service/name ?service]
                             [?m :metric/host ?h]
                             [?m :metric/service ?s]]
                           db host service))
          [host-id host-txn] (get-or-create db :host/name host)
          [service-id service-txn] (get-or-create db :service/name service)
          tempid (d/tempid :db.part/db)
          txn (when-not in-db
                (concat
                  host-txn
                  service-txn
                  [{:db/id tempid
                    :metric/host host-id
                    :metric/service service-id
                    :db.install/_partition :db.part/db}]))
          resolved-id (when txn
                        (d/resolve-tempid
                          db 
                          (:tempids @(d/transact conn txn))
                          tempid))]
      (or in-db resolved-id))))

(comment
  (get-or-create-metric conn {:host "example.com" :service "cpu"})

  (get-or-create-metric conn {:host "example.com" :service "lol"})
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
            ;; Could fail
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
                            tx (when (and metric-id (:metric e))
                                 {:db/id (d/tempid metric-id)
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
      ;;Could throw
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

  (q '[:find ?time ?value
       :in $ ?host ?service ?from
       :where
       [?h :host/name ?host]
       [?s :service/name ?service]
       [?m :metric/host ?h]
       [?m :metric/service ?s]
       [?m :metric/tick ?t]
       [?t :tick/value ?value]    
       [?t :tick/time ?t-raw]
       [(.getTime ^java.util.Date ?t-raw) ?time]
       [(> ?time ?from)]]
     (db conn) "localhost" "memory" (- (System/currentTimeMillis) (* 60 1000 10))) 
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

(defn metric-ts-q
  "Finds the metric timeseries from the given metric between the requested start and end.
   
   This should be fast."
  [db metric start end]
  (let [start-id (d/entid-at db metric start)
        end-ms (.getTime ^Date end)
        s (->> (d/seek-datoms db :eavt start-id)
               (take-while (fn [datom]
                             (-> (:e datom)
                                 (d/part)
                                 (= metric))))
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

(defn zero-gaps
  "Takes a sequence of samples and start and end times, and returns a
   sequence that includes all the given data, plus zero-valued samples
   just before or just after values that had a gap that was 'significant'
   according to the given parameter"
  [start end significant-gap samples]
  ;;TODO: implement
  )

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
                                                           (cons [nil (quot (.getTime start) 1000)]))
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
