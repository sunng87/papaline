(ns papaline.core
  (:require [clojure.core.async :as a
             :refer :all
             :exclude [partition-by map into reduce partition take merge]]))

(defn stage [stage-fn & {:keys [buffer-size
                                buffer-type]
                         :or {buffer-size 100}}]
  (let [buffer-fn (case buffer-type
                    :sliding sliding-buffer
                    :dropping dropping-buffer
                    buffer)
        in-chan (chan (buffer-fn buffer-size))]
    [(fn [out-chan done-chan]
       (go-loop []
                (let [[ctx port] (alts! [done-chan in-chan] :priority true)]
                  (if (not= port done-chan)
                    (do
                      (go
                       (let [result (apply stage-fn (:args ctx))
                             ctx (assoc ctx :args result)]
                         (if out-chan
                           (>! out-chan ctx)
                           (when (:wait ctx)
                             (>! (:wait ctx) ctx)))))
                      (recur))
                    (close! in-chan)))))
     in-chan]))

(defn copy-stage [stage-fn & options]
  (let [sfn (fn [& args]
              (apply stage-fn args)
              args)]
    (apply stage sfn options)))

(defn pipeline [stages]
  (let [entry (-> stages first second)
        done (chan)]
    (loop [stages* stages]
      (when (first stages*)
        (let [[stage* in] (first stages*)
              [_ out] (second stages*)]
          (stage* out done)
          (recur (rest stages*)))))
    [(fn [call-info]
       (go
        (>! entry call-info)))
     done]))

(defn run-pipeline [pipeline & args]
  ((first pipeline) {:args args}))

(defn run-pipeline-wait [pipeline & args]
  (let [sync-chan (chan)]
    ((first pipeline) {:args args
                       :wait sync-chan})
    (:args (first (alts!! [(second pipeline) sync-chan])))))

(defn run-pipeline-timeout [pipeline timeout-interval timeout-val & args]
  (let [sync-chan (chan)
        timeout-chan (timeout timeout-interval)]
    ((first pipeline) {:args args
                       :wait sync-chan})
    (let [done-chan (second pipeline)
          [v port] (alts!! [done-chan timeout-chan sync-chan] :priority true)]
      (if (= port timeout-chan)
        timeout-val
        (:args v)))))

(defn cancel-pipeline [pipeline]
  (>!! (second pipeline) 0))
