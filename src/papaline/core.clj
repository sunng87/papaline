(ns papaline.core
  (:require [clojure.core.async :as a
             :refer :all
             :exclude [partition-by map into reduce partition take merge
                       pipeline]]))

(defrecord Stage [buffer-factory stage-fn])
(defrecord RealizedStage [buffer stage-fn])

(defn stage [stage-fn & {:keys [in-chan
                                buffer-size
                                buffer-type]
                         :or {buffer-size 100}}]
  (let [buffer-fn (case buffer-type
                    :sliding sliding-buffer
                    :dropping dropping-buffer
                    buffer)
        buffer-factory #(chan (buffer-fn buffer-size))]
    (Stage. buffer-factory stage-fn)))

(defn copy-stage [stage-fn & options]
  (let [sfn (fn [& args]
              (apply stage-fn args)
              args)]
    (apply stage sfn options)))

(defrecord Pipeline [entry-fn done-chan stages])

(defn pipeline [stages & {:keys [error-handler]}]
  (let [stages (mapv #(if (fn? %) (stage %) %) stages)
        realized-stages (mapv #(RealizedStage. ((.buffer-factory %))
                                               (.stage-fn %))
                              stages)
        entry (-> realized-stages first (.buffer))
        done-chan (chan)]
    (loop [stages* realized-stages]
      (when (first stages*)
        (let [in-chan (.buffer (first stages*))
              task (.stage-fn (first stages*))
              out-chan (when (second stages*) (.buffer (second stages*)))]
          (go-loop []
                   (let [[ctx port] (alts! [done-chan in-chan] :priority true)]
                     (if (not= port done-chan)
                       (do
                         (go
                           (let [ctx (try
                                       (let [args (:args ctx)
                                             args (if (or (nil? args) ;; empty arguments
                                                          (sequential? args))
                                                    args [args])]
                                         (assoc ctx :args (apply task args)))
                                       (catch Exception e
                                         (if (and (instance? clojure.lang.ExceptionInfo e)
                                                  (:abort (ex-data e)))
                                           (merge ctx (ex-data e))
                                           (do
                                             (when error-handler
                                               (error-handler e (:args ctx)))
                                             (when (:error ctx)
                                               (>! (:error ctx) e))))))
                                 out-chan (or out-chan (:wait ctx))]
                             (when out-chan
                               (cond
                                (:abort ctx)
                                (when (:wait ctx) (>! (:wait ctx) ctx))

                                ;; the results are forked
                                (:fork (meta (:args ctx)))
                                (let [ctx (-> ctx
                                              (update-in [:forks]
                                                         #(conj (or % []) (count @(:args ctx))))
                                              (update-in  [:fork-rets]
                                                          #(conj (or % []) (atom []))))]
                                  (doseq [forked-args @(:args ctx)]
                                    (>! out-chan (assoc ctx :args forked-args))))

                                ;; this tasks requires join
                                (:join (meta (:args ctx)))
                                (let [fork-rets (swap! (last (:fork-rets ctx)) conj @(:args ctx))]
                                  (when (= (last (:forks ctx)) (count fork-rets))
                                    (>! out-chan (assoc ctx
                                                   :args fork-rets
                                                   :forks (vec (drop-last (:forks ctx)))
                                                   :fork-rets (vec (drop-last (:fork-rets ctx)))))))

                                ;; normal linear
                                :else (>! out-chan ctx)))))
                         (recur))
                       (close! in-chan))))
          (recur (rest stages*)))))
    (Pipeline. (fn [call-info]
                 (go
                   (>! entry call-info)))
               done-chan
               realized-stages)))

(defn run-pipeline [pipeline & args]
  ((.entry-fn pipeline) {:args args}))

(defn run-pipeline-wait [pipeline & args]
  (let [sync-chan (chan)
        error-chan (chan)]
    ((.entry-fn pipeline) {:args args
                           :wait sync-chan
                           :error error-chan})
    (let [done-chan (.done-chan pipeline)
          [result port] (alts!! [done-chan error-chan sync-chan] :priority true)]
      (condp = port
        sync-chan (:args result)
        error-chan (throw result)
        done-chan (throw (ex-info "Pipeline closed"))))))

(defn run-pipeline-timeout [pipeline timeout-interval timeout-val & args]
  (let [sync-chan (chan)
        error-chan (chan)
        timeout-chan (timeout timeout-interval)]
    ((.entry-fn pipeline) {:args args
                           :wait sync-chan
                           :error error-chan})
    (let [done-chan (.done-chan pipeline)
          [result port] (alts!! [done-chan timeout-chan error-chan sync-chan] :priority true)]
      (condp = port
        done-chan (throw (ex-info "Pipeline closed"))
        sync-chan (:args result)
        error-chan (throw result)
        timeout-chan timeout-val))))

(defn pipeline-stage
  "pipeline as a stage"
  [pipeline]
  (let [stages (.stages pipeline)
        in-chan (.buffer (first stages))]
    (stage (fn [& args]
             (apply run-pipeline-wait pipeline args))
           :in-chan in-chan)))

(defn cancel-pipeline [pipeline]
  (>!! (.done-chan pipeline) 0))

(deftype MetadataObj [val meta-map-wrapper]
  clojure.lang.IDeref
  (deref [this]
    val)

  clojure.lang.IObj
  (withMeta [this m]
    (swap! meta-map-wrapper merge m)
    this)

  clojure.lang.IMeta
  (meta [this]
    @meta-map-wrapper))

(defn- meta-obj [v]
  (MetadataObj. v (atom {})))

(defn- assoc-meta [v & args]
  (with-meta v (apply assoc (or (meta v) {}) args)))

(defn abort
  ([] (throw (ex-info "Aborted" {:abort true})))
  ([ret] (throw (ex-info "Aborted" {:abort true :ret ret}))))

(defn fork [ret]
  (when-not (sequential? ret)
    (throw (IllegalArgumentException. "Only sequential value is forkable.")))
  (assoc-meta (meta-obj ret) :fork true))

(defn join [ret]
  (assoc-meta (meta-obj ret) :join true))
