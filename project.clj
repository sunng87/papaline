(defproject info.sunng/papaline "0.9.1-SNAPSHOT"
  :description "Clojure concurrent pipeline on core.async"
  :url "http://github.com/sunng87/papaline"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/core.async "0.3.465"]]
  :scm {:url "https://github.com/sunng87/papaline"
        :name "github"}
  :java-source-paths ["java_src"]
  :deploy-repositories {"releases" :clojars})
