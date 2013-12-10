(defproject riemann-enhancements "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :resource-paths ["resource"]
  :jvm-opts ["-XX:-OmitStackTraceInFastThrow"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [ring "1.2.1"]
                 [ring/ring-json "0.2.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [aleph "0.3.0"]
                 [compojure "1.1.6"]
                 [narrator "0.1.0"]
                 [ring/ring-devel "1.2.1"]
                 [com.datomic/datomic-free "0.9.4331"
                  :exclusions [org.slf4j/log4j-over-slf4j]]])
