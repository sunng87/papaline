(defproject info.sunng/papaline "0.3.3-SNAPSHOT"
  :description "Clojure concurrent pipeline on core.async"
  :url "http://github.com/sunng87/papaline"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.301.0-deb34a-alpha"]]
  :scm {:url "https://github.com/sunng87/papaline"
        :name "github"}
  :deploy-repositories {"releases" :clojars})
