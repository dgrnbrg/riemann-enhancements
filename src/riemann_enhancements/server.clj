(ns riemann-enhancements.server
  (:import java.util.Date)
  (:require [riemann-enhancements.core :refer :all]
            [riemann.service :refer (Service ServiceEquiv)]
            [clojure.tools.logging :as log]
            [aleph.http :as aleph]))

(defrecord RingServer [host port handler core server]
  ServiceEquiv
  (equiv? [this other]
          (and (instance? RingServer other)
               (= host (:host other))
               (= port (:port other))  
               (= handler (:handler other))))

  Service
  (conflict? [this other]
             (and (instance? RingServer other)
                  (= host (:host other))
                  (= port (:port other))
                  (= handler (:handler other))))  

  (reload! [this new-core]
           (reset! core new-core))

  (start! [this]
          (locking this
            (when-not @server
              (reset! server (aleph/start-http-server
                               (aleph/wrap-ring-handler handler)
                               {:host host
                                :port port}))
              (log/info "Ring server" host port "online"))))

  (stop! [this]
         (locking this
           (when @server
             (@server)
             (log/info "Ring server" host port "shut down")))))

(defn ring-server
  "Starts a new websocket server for a core. Starts immediately.

  Options:
  :host   The address to listen on (default 127.0.0.1)
          Currently does nothing; this option depends on an incomplete
          feature in Aleph, the underlying networking library. Aleph will
          currently bind to all interfaces, regardless of this value.
  :port   The port to listen on (default 5559)"
  ([handler & opts]
   (->RingServer
     (get opts :host "127.0.0.1")
     (get opts :port 5559)
     handler
     (atom nil)
     (atom nil))))
