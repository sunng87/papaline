(ns papaline.core
  (:require [clojure.core.async :as a
             :refer :all
             :exclude [partition-by map into reduce partition take merge
                       pipeline transduce]]
            [papaline.util :refer [defprotocol+ defrecord+]])
  (:import [papaline.concurrent ArgumentsAwareCallable]
           [java.util.concurrent ExecutorService TimeUnit TimeoutException Callable
                                 ThreadPoolExecutor LinkedBlockingQueue RejectedExecutionHandler
                                 ThreadPoolExecutor$DiscardOldestPolicy ThreadFactory Future CompletableFuture CompletionException]
           (java.util.function BiFunction)))

(defrecord Stage [buffer-factory stage-fn]
  clojure.lang.IFn
  (invoke [this args]
    (apply stage-fn args)))

(defrecord RealizedStage [buffer stage-fn name]
  clojure.lang.IFn
  (applyTo [this args]
    (apply stage-fn args)))

(defn start-stage [^Stage s]
  (RealizedStage. ((.buffer-factory s)) (.stage-fn s) (:name s)))

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

(defn named-stage [name & args]
  (assoc (apply stage args) :name name))

(defn copy-stage [stage-fn & options]
  (let [sfn (fn [& args]
              (apply stage-fn args)
              args)]
    (apply stage sfn options)))

(def ^:dynamic *pre-execution-hook* nil)
(def ^:dynamic *post-execution-hook* nil)

(defprotocol+ IPipeline
  (start! [this])
  (run-pipeline [this & args])
  (run-pipeline-wait [this & args])
  (run-pipeline-timeout [this timeout-interval timeout-val & args])
  (stop! [this]))

(defn- process-exception [stage-name error-handler ctx args e]
  (let [cause (if (instance? CompletionException e) (.getCause ^Throwable e) e)]
    (if (and (instance? clojure.lang.ExceptionInfo cause)
             (:abort (ex-data cause)))
      (merge ctx (ex-data cause))
      (let [ex (ex-info "Papaline stage error"
                        {:args  args
                         :stage stage-name} e)]
        (when error-handler
          (error-handler ex))
        (assoc ctx :ex ex)))))

(defn- run-task [^RealizedStage current-stage ctx error-handler]
  (let [task (.stage-fn current-stage)
        args (:args ctx)
        args (if (or (nil? args) ;; empty arguments
                     (sequential? args))
               args [args])]
    (try
      (assoc ctx :args (apply task args))
      (catch Exception e
        (process-exception (:name current-stage) error-handler ctx args e)))))

(defrecord+ Pipeline [done-chan stages error-handler]
  clojure.lang.IFn
  (invoke [this ctx]
          (let [entry (.buffer ^RealizedStage (first stages))]
            (go
              (when *pre-execution-hook*
                (*pre-execution-hook* ctx))
              (>! entry (assoc ctx :post-hook *post-execution-hook*)))))

  IPipeline
  (start! [this]
          (loop [stages* stages]
            (when (first stages*)
              (let [^RealizedStage current-stage (first stages*)
                    in-chan (.buffer current-stage)
                    out-chan (when (second stages*) (.buffer ^RealizedStage (second stages*)))]
                (go-loop []
                  (let [[ctx port] (alts! [done-chan in-chan] :priority true)]
                    (if (not= port done-chan)
                      (do
                        (go
                          (let [ctx (run-task current-stage ctx error-handler)]
                            (when (and (:error ctx) (:ex ctx))
                              (>! (:error ctx) (:ex ctx)))
                            (when-not out-chan
                              (when-let [post-hook (:post-hook ctx)]
                                (post-hook ctx)))
                            (when-let [out-chan (or out-chan (:wait ctx))]
                              (cond
                               (:abort ctx)
                               (do
                                 (when (:wait ctx) (>! (:wait ctx) ctx))
                                 (when-let [post-hook (:post-hook ctx)] (post-hook ctx)))

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
                (recur (rest stages*))))))

  (run-pipeline [this & args]
                (this {:args args}))

  (run-pipeline-wait [this & args]
                     (let [sync-chan (chan)
                           error-chan (chan)]
                       (this {:args args
                              :wait sync-chan
                              :error error-chan})
                       (let [[result port] (alts!! [done-chan error-chan sync-chan] :priority true)]
                         (condp = port
                           sync-chan (:args result)
                           error-chan (throw result)
                           done-chan (throw (ex-info "Pipeline closed"))))))

  (run-pipeline-timeout [this timeout-interval timeout-val & args]
                        (let [sync-chan (chan)
                              error-chan (chan)
                              timeout-chan (timeout timeout-interval)]
                          (this {:args args
                                 :wait sync-chan
                                 :error error-chan})
                          (let [[result port] (alts!! [done-chan timeout-chan error-chan sync-chan] :priority true)]
                            (condp = port
                              done-chan (throw (ex-info "Pipeline closed"))
                              sync-chan (:args result)
                              error-chan (throw result)
                              timeout-chan timeout-val))))
  (stop! [this]
         (>!! done-chan 0)))

(defn- ^Callable wrap-with-arguments-aware-callable [callable args-array]
  (ArgumentsAwareCallable. ^Callable callable ^"[Ljava.lang.Object;" args-array))

(defrecord+ DedicatedThreadPoolPipeline [executor stages error-handler]
  clojure.lang.IFn
  (invoke [this ctx]
          (let [pre-hook *pre-execution-hook*
                post-hook *post-execution-hook*
                clos (fn []
                       (when pre-hook (pre-hook ctx))
                       (loop [stgs stages ctx ctx]
                         (if-let [s (first stgs)]
                           (let [ctx (run-task s ctx error-handler)]
                             (cond
                               (:abort ctx)
                               (do
                                 (when post-hook (post-hook ctx))
                                 ctx)

                               (:fork (meta (:args ctx)))
                               (when error-handler
                                 (error-handler (UnsupportedOperationException. "Fork is not supported in DedicatedThreadPoolPipeline")))

                               (:join (meta (:args ctx)))
                               (when error-handler
                                 (error-handler (UnsupportedOperationException. "Join is not supported in DedicatedThreadPoolPipeline")))
                               :else (recur (rest stgs) ctx)))
                           (do
                             (when post-hook (post-hook ctx))
                             ctx))))
                clos-wrapper (wrap-with-arguments-aware-callable clos
                                                                 (if (not-empty (:args ctx))
                                                                   (.toArray ^clojure.lang.ArraySeq (:args ctx))
                                                                   (into-array [])))]
            (.submit ^ExecutorService executor clos-wrapper)))

  IPipeline
  (start! [this])

  (run-pipeline [this & args]
                (this {:args args}))

  (run-pipeline-wait [this & args]
                     (let [^Future future (this {:args args})]
                       (:args (.get future))))

  (run-pipeline-timeout [this timeout-interval timeout-val & args]
                        (let [^Future future (this {:args args})]
                          (try
                            (:args (.get future timeout-interval TimeUnit/MILLISECONDS))
                            (catch TimeoutException e
                              timeout-val))))
  (stop! [this]))

(defn- ^CompletableFuture run-stage* [^RealizedStage current-stage ctx]
  (let [task (.stage-fn current-stage)
        args (:args ctx)
        args (if (or (nil? args) ;; empty arguments
                     (sequential? args))
               args [args])]
    (try
      (let [next-args (apply task args)]
        (if (instance? CompletableFuture next-args)
          next-args
          (CompletableFuture/completedFuture next-args)))
      (catch Exception e
        (let [f (CompletableFuture.)]
          (.completeExceptionally f e)
          f)))))

(defn- get-next-ctx [ctx next-args ex error-handler]
  (if (some? ex)
    (error-handler ctx (:args ctx) ex)
    (assoc ctx :args next-args)))

(defrecord+ AsyncDedicatedThreadPoolPipeline [executor stages error-handler]
  clojure.lang.IFn
  (invoke [this ctx]
          (let [pre-hook  *pre-execution-hook*
                post-hook *post-execution-hook*
                result (CompletableFuture.)
                args-array (if (not-empty (:args ctx))
                             (.toArray ^clojure.lang.ArraySeq (:args ctx))
                             (into-array []))]
            (letfn [(run-stages [stgs ctx]
                      (if-let [^RealizedStage current-stage (first stgs)]
                        (let [current-stage-name (:name current-stage)
                              stage-name-aware-error-handler (partial process-exception current-stage-name error-handler)
                              processor (Thread/currentThread)]
                          (.handle (run-stage* current-stage ctx)
                                   (reify BiFunction
                                     (apply [_ next-args ex]
                                       (if (= processor (Thread/currentThread))
                                         (-> (get-next-ctx ctx next-args ex stage-name-aware-error-handler)
                                             (process-result stgs))
                                         (.submit ^ExecutorService executor
                                                  (wrap-with-arguments-aware-callable
                                                    (fn []
                                                      (-> (get-next-ctx ctx next-args ex stage-name-aware-error-handler)
                                                          (process-result stgs)))
                                                    args-array)))))))
                        (do
                          (when post-hook (post-hook ctx))
                          (.complete result ctx))))
                    (process-result [ctx stgs]
                      (let [args-meta (meta (:args ctx))
                            use-fork-or-join? (or (:fork args-meta) (:join args-meta))]
                        (if (or (:abort ctx) use-fork-or-join?)
                          (do
                            (when post-hook (post-hook ctx))
                            (when (and error-handler use-fork-or-join?)
                              (try
                                (error-handler (UnsupportedOperationException. "Fork or Join is not supported in AsyncDedicatedThreadPoolPipeline"))
                                (catch Exception _)))
                            (.complete result ctx))
                          (run-stages (rest stgs) ctx))))]
              (.submit ^ExecutorService executor (wrap-with-arguments-aware-callable (fn []
                                                                                       (when pre-hook (pre-hook ctx))
                                                                                       (run-stages stages ctx))
                                                                                     args-array)))
            result))
  IPipeline
  (start! [this])

  (run-pipeline [this & args]
                (this {:args args}))

  (run-pipeline-wait [this & args]
                     (let [future (this {:args args})]
                       (:args (.get ^Future future))))

  (run-pipeline-timeout [this timeout-interval timeout-val & args]
                        (let [future (this {:args args})]
                          (try
                            (:args (.get ^Future future timeout-interval TimeUnit/MILLISECONDS))
                            (catch TimeoutException e
                              timeout-val))))
  (stop! [this]))

(defn pipeline [stages & {:keys [error-handler]}]
  (let [realized-stages (mapv start-stage stages)
        done-chan (chan)]
    (doto (Pipeline. done-chan realized-stages error-handler)
      (start!))))

(defn pipeline-stage
  "pipeline as a stage"
  [pipeline]
  (let [stages (.stages ^Pipeline pipeline)
        in-chan (.buffer ^RealizedStage (first stages))]
    (stage (fn [& args]
             (apply run-pipeline-wait pipeline args))
           :in-chan in-chan)))

(defn cancel-pipeline [pipeline]
  (stop! pipeline))

(defn counted-thread-factory
  [name-format daemon]
  (let [counter (atom 0)]
    (reify
      ThreadFactory
      (newThread [this runnable]
        (doto (Thread. runnable)
          (.setName (format name-format (swap! counter inc)))
          (.setDaemon daemon))))))

(defn make-thread-pool [threads queue-size & {:keys [overflow-action]
                                              :or {overflow-action (ThreadPoolExecutor$DiscardOldestPolicy.)}}]
  (ThreadPoolExecutor. (int threads) (int threads) (long 0)
                       TimeUnit/MILLISECONDS
                       (LinkedBlockingQueue. ^long queue-size)
                       (counted-thread-factory "papaline-pool-%d" true)
                       ^RejectedExecutionHandler overflow-action))

(defn dedicated-thread-pool-pipeline [stages thread-pool & {:keys [error-handler]}]
  (DedicatedThreadPoolPipeline. thread-pool (mapv start-stage stages) error-handler))

(defn async-dedicated-thread-pool-pipeline [stages thread-pool & {:keys [error-handler]}]
  (AsyncDedicatedThreadPoolPipeline. thread-pool (mapv start-stage stages) error-handler))

(defrecord+ CompoundPipeline [pipelines]
  clojure.lang.IFn
  (invoke [this ctx]
    (loop [ps pipelines ctx ctx]
      (if-let [p (first ps)]
        ;; FIXME: do not block here
        (let [ctx (apply run-pipeline-wait p (:args ctx))]
          (cond
            (:abort ctx) ctx
            (:ex ctx) ctx
            (:fork (meta (:args ctx)))
            (when-let [error-handler (:error-handler p)]
              (error-handler (UnsupportedOperationException.
                               "Fork is not supported for now")))
            (:join (meta (:args ctx)))
            (when-let [error-handler (:error-handler p)]
              (error-handler (UnsupportedOperationException.
                               "Join is not supported for now ")))
            :else (recur (rest ps) ctx)))
        ctx)))

  IPipeline
  (start! [this])

  (run-pipeline [this & args]
    (this {:args args}))

  (run-pipeline-wait [this & args]
    (let [^Future future (this {:args args})]
      (:args (.get future))))

  (run-pipeline-timeout [this timeout-interval timeout-val & args]
    (let [^Future future (this {:args args})]
      (try
        (:args (.get future timeout-interval TimeUnit/MILLISECONDS))
        (catch TimeoutException e
          timeout-val))))
  (stop! [this]))

(defn compound-pipeline [pipelines]
  (CompoundPipeline. pipelines))

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
  ([ret] (throw (ex-info "Aborted" {:abort true :args ret}))))

(defn fork [ret]
  (when-not (sequential? ret)
    (throw (IllegalArgumentException. "Only sequential value is forkable.")))
  (assoc-meta (meta-obj ret) :fork true))

(defn join [ret]
  (assoc-meta (meta-obj ret) :join true))
