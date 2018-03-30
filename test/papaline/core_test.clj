(ns papaline.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan >!! <!! go timeout]]
            [papaline.core :refer :all])
  (:import (java.util.concurrent CompletableFuture)
           (java.util.function Supplier)))

(deftest test-stage
  (let [f (fn [arg])
        the-stage (stage f)]
    (is (fn? (.stage-fn the-stage)))))

(deftest test-pipeline
  (let [f (take 5 (repeat (fn [c] (swap! c inc))))
        stgs (map stage f)
        ppl (pipeline stgs)]
    (is (instance? clojure.lang.IFn ppl))))

(deftest test-run-pipeline
  (let [c0 (atom 0)
        sync-chan (chan)
        num 5
        f (vec (take num (repeat (fn [c sync-chan] (swap! c inc) [c sync-chan]))))
        f (conj f (fn [c sync-chan] (>!! sync-chan 0)))
        stgs (map stage f)
        ppl (pipeline stgs)]
    (run-pipeline ppl c0 sync-chan)
    (<!! sync-chan)
    (is (= num @c0))))

(deftest test-run-threadpool-pipeline
  (let [c0 (atom 0)
        sync-chan (chan)
        num 5
        executor (make-thread-pool 2 2)
        f (vec (take num (repeat (fn [c sync-chan] (swap! c inc) [c sync-chan]))))
        f (conj f (fn [c sync-chan] (>!! sync-chan 0)))
        stgs (map stage f)
        ppl (dedicated-thread-pool-pipeline stgs executor)
        ret (run-pipeline ppl c0 sync-chan)]
    ;(run-pipeline ppl c0 sync-chan)
    (<!! sync-chan)
    (is (= num @c0))
    (is (= (.get ret) {:args true}))))

(deftest test-run-async-threadpool-pipeline
  (let [c0 (atom 0)
        sync-chan (chan)
        num 5
        executor (make-thread-pool 2 2)
        f (vec (take num (repeat (fn [c sync-chan]
                                   (swap! c inc)
                                   (CompletableFuture/supplyAsync (reify Supplier
                                                                    (get [this]
                                                                      (Thread/sleep 300)
                                                                      [c sync-chan])))))))
        f (conj f (fn [c sync-chan] (CompletableFuture/completedFuture (>!! sync-chan 0))))
        stgs (map stage f)
        ppl (async-dedicated-thread-pool-pipeline stgs executor)
        ret (run-pipeline ppl c0 sync-chan)]
    (<!! sync-chan)
    (is (= num @c0))
    (is (= (.get ret) {:args true}))))

(deftest test-copy-stage
  (let [c0 (atom 0)
        sync-chan (chan)
        num 5
        stgs (vec (map copy-stage (take num (repeat (fn [c sync-chan] (swap! c inc))))))
        stgs (conj stgs (stage (fn [c sync-chan]  (>!! sync-chan 0))))
        ppl (pipeline stgs)]
    (run-pipeline ppl c0 sync-chan)
    (<!! sync-chan)
    (is (= num @c0))))

(deftest test-pipeline-wait
  (let [c0 (atom 0)
        num 5
        stgs (vec (map copy-stage (take num (repeat (fn [c] (swap! c inc))))))
        ppl (pipeline stgs)]
    (run-pipeline-wait ppl c0)
    (is (= num @c0))))

(deftest test-pipeline-timeout
  (let [num 2
        stgs (vec (map copy-stage (take num (repeat (fn [] (<!! (timeout 2000)))))))
        ppl (pipeline stgs)]
    (is (= :timeout (run-pipeline-timeout ppl 1000 :timeout)))
    (cancel-pipeline ppl)))

(deftest test-pipeline-stage
  (let [c0 (atom 0)
        num 5
        stgs (vec (map copy-stage (take num (repeat (fn [c] (swap! c inc))))))
        ppl (pipeline stgs)]
    (run-pipeline-wait (pipeline [(pipeline-stage ppl)]) c0)
    (is (= num @c0))))

(deftest test-abort
  (let [c0 (atom 0)
        stg0 (fn [c] (abort))
        stg1 (fn [c] (swap! c inc))
        normal-stgs (map stage [stg0 stg1])

        async-stg0 (fn [c] (CompletableFuture/supplyAsync (reify Supplier (get [this] (abort)))))
        async-stg1 (fn [c] (swap! c inc) (CompletableFuture/completedFuture "Done"))
        async-stgs (map stage [async-stg0 async-stg1])

        ppl (pipeline normal-stgs)
        ppl2 (dedicated-thread-pool-pipeline normal-stgs (make-thread-pool 2 2))
        ppl3 (async-dedicated-thread-pool-pipeline normal-stgs (make-thread-pool 2 2))
        ppl4 (async-dedicated-thread-pool-pipeline async-stgs (make-thread-pool 2 2))]
    (run-pipeline-wait ppl c0)
    (is (= 0 @c0))

    (run-pipeline-wait ppl2 c0)
    (is (= 0 @c0))

    (run-pipeline-wait ppl3 c0)
    (is (= 0 @c0))

    (run-pipeline-wait ppl4 c0)
    (is (= 0 @c0))))

(deftest test-abort-and-return
  (testing "abort with return value"
    (let [c0 (atom 0)
          stg0 (fn [c] (abort -1))
          stg1 (fn [c] (swap! c inc))
          normal-stgs (map stage [stg0 stg1])

          async-stg0 (fn [c] (CompletableFuture/supplyAsync (reify Supplier (get [this] (abort -1)))))
          async-stg1 (fn [c] (swap! c inc) (CompletableFuture/completedFuture "Done"))
          async-stgs (map stage [async-stg0 async-stg1])

          ppl (pipeline (map stage [stg0 stg1]))
          ppl2 (dedicated-thread-pool-pipeline (map stage [stg0 stg1])
                                               (make-thread-pool 2 2))
          ppl3 (async-dedicated-thread-pool-pipeline normal-stgs (make-thread-pool 2 2))
          ppl4 (async-dedicated-thread-pool-pipeline async-stgs (make-thread-pool 2 2))]
      (is (= -1 (run-pipeline-wait ppl c0)))
      (is (= 0 @c0))

      (is (= -1 (run-pipeline-wait ppl2 c0)))
      (is (= 0 @c0))

      (is (= -1 (run-pipeline-wait ppl3 c0)))
      (is (= 0 @c0))

      (is (= -1 (run-pipeline-wait ppl4 c0)))
      (is (= 0 @c0)))))

(deftest test-fork-join
  (let [fork-stage (fn [] (fork (take 5 (repeat [1]))))
        join-inc-stage (fn [i] (join (inc i)))
        combine-stage (fn [& l] (reduce + l))]
    (is (= 10 (run-pipeline-wait
               (pipeline (map stage [fork-stage join-inc-stage combine-stage])))))
    (is (= [2 2 2 2 2]
           (run-pipeline-wait
            (pipeline (map stage [fork-stage join-inc-stage])))))))

(deftest test-args-auto-vec
  (let [fff (fn [] 1)
        ff2 (fn [a] (inc a))]
    (is (= 2 (run-pipeline-wait (pipeline (map stage [fff ff2])))))))

(deftest test-error-handler-sync
  (let [e (atom 0)
        f (fn [] (throw (ex-info "expected error." {})))
        p (pipeline (map stage [f]) :error-handler (fn [_] (swap! e inc)))]
    (try (run-pipeline-wait p) (catch Exception e))
    (is (= 1 @e))))

(deftest test-error-handler-async
  (let [e (atom 0)
        sync (chan)
        f (fn [] (throw (ex-info "expected error" {})))
        p (pipeline (map stage [f]) :error-handler (fn [_] (swap! e inc) (>!! sync 1)))]
    (run-pipeline p)
    (<!! sync)
    (is (= 1 @e))))

(deftest test-error-handler-with-name
  (let [stage-name "demo-stage-0"
        f (named-stage stage-name (fn [] (throw (ex-info "expected error" {}))))
        p (pipeline [f] :error-handler
                    (fn [e]
                      (is (= stage-name (:stage (ex-data e))))))]
    (try (run-pipeline-wait p) (catch Exception e))))

(deftest test-stage-invokable
  (is (= 2 ((stage inc) [1]))))

(deftest test-fork-join-in-dedicated-threadpool-pipeline
  (testing "dedicated thread pool doesn't accept (fork) and (join) result, error expected"
    (let [mark (atom 0)
          t (make-thread-pool 2 2)
          f (named-stage "demo-stage-0" (fn [] (fork (range 10))))
          p (dedicated-thread-pool-pipeline [f] t
                                            :error-handler
                                            (fn [e]
                                              (is (instance? UnsupportedOperationException e))
                                              (swap! mark inc)))]
      (run-pipeline-wait p)
      (is (= 1 @mark)))))

(deftest test-fork-join-in-async-dedicated-threadpool-pipeline
  (testing "async dedicated thread pool doesn't accept (fork) and (join) result, error expected"
    (let [mark (atom 0)
          t    (make-thread-pool 2 2)
          f    (named-stage "demo-stage-0" (fn [] (fork (range 10))))
          p    (async-dedicated-thread-pool-pipeline [f] t
                                                     :error-handler
                                                     (fn [e]
                                                       (is (instance? UnsupportedOperationException e))
                                                       (swap! mark inc)))]
      (run-pipeline-wait p)
      (is (= 1 @mark)))))

(deftest test-pre-post-hook-for-pipeline
  (testing "pre-hook and post-hook"
    (let [h0 (atom false)
          h1 (atom false)
          c0 (atom 0)
          num 5
          stgs (vec (map copy-stage (take num (repeat (fn [c] (swap! c inc))))))
          ppl (pipeline stgs)]
      (binding [*pre-execution-hook* (fn [_] (reset! h0 true))
                *post-execution-hook* (fn [_] (reset! h1 true))]
        (run-pipeline-wait ppl c0)
        (is (= num @c0))
        (is (and @h0 @h1))))
    (let [h0 (atom false)
          h1 (atom false)
          c0 (atom 0)
          sync-chan (chan)
          num 5
          f (vec (take num (repeat (fn [c sync-chan] (swap! c inc) [c sync-chan]))))
          f (conj f (fn [c sync-chan] (>!! sync-chan 0)))
          stgs (map stage f)
          ppl (pipeline stgs)]
      (binding [*pre-execution-hook* (fn [_] (reset! h0 true))
                *post-execution-hook* (fn [_] (reset! h1 true))]
        (run-pipeline ppl c0 sync-chan)
        (<!! sync-chan)
        (is (= num @c0))
        (is (and @h0 @h1)))))
  (testing "for dedicated thread pool"
    (let [h0 (atom false)
          h1 (atom false)
          c0 (atom 0)
          sync-chan (chan)
          num 5
          executor (make-thread-pool 2 2)
          f (vec (take num (repeat (fn [c sync-chan] (swap! c inc) [c sync-chan]))))
          f (conj f (fn [c sync-chan] (>!! sync-chan 0)))
          stgs (map stage f)
          ppl (dedicated-thread-pool-pipeline stgs executor )]
      (binding [*pre-execution-hook* (fn [_] (reset! h0 true))
                *post-execution-hook* (fn [_] (reset! h1 true))]
        (run-pipeline ppl c0 sync-chan)
        (<!! sync-chan)
        (is (= num @c0))
        (is (and @h0 @h1)))))
  (testing "for async dedicated thread pool"
    (let [h0 (atom false)
          h1 (atom false)
          c0 (atom 0)
          sync-chan (chan)
          num 5
          executor (make-thread-pool 2 2)
          f (vec (take num (repeat (fn [c sync-chan] (swap! c inc) [c sync-chan]))))
          f (conj f (fn [c sync-chan] (>!! sync-chan 0)))
          stgs (map stage f)
          ppl (async-dedicated-thread-pool-pipeline stgs executor)]
      (binding [*pre-execution-hook* (fn [_] (reset! h0 true))
                *post-execution-hook* (fn [_] (reset! h1 true))]
        (run-pipeline ppl c0 sync-chan)
        (<!! sync-chan)
        (is (= num @c0))
        (is (and @h0 @h1)))))
  (testing "abort"
    (let [c0 (atom 0)
          h0 (atom 0)
          h1 (atom 0)
          stg0 (fn [c] (abort))
          stg1 (fn [c] (swap! c inc))
          ppl (pipeline (map stage [stg0 stg1]))
          ppl2 (dedicated-thread-pool-pipeline (map stage [stg0 stg1])
                                               (make-thread-pool 2 2))
          ppl3 (async-dedicated-thread-pool-pipeline (map stage [stg0 stg1])
                                                     (make-thread-pool 2 2))]
      (binding [*pre-execution-hook* (fn [_] (swap! h0 inc))
                *post-execution-hook* (fn [_] (swap! h1 inc))]
        (run-pipeline-wait ppl c0)
        (is (= 0 @c0))

        (run-pipeline-wait ppl2 c0)
        (is (= 0 @c0))

        (run-pipeline-wait ppl3 c0)
        (is (= 0 @c0))

        (is (= 3 @h0))
        (is (= 3 @h1)))))
  (testing "error-handler"
    (let [h0 (atom false)
          h1 (atom false)
          e (atom 0)
          f (fn [] (throw (ex-info "expected error." {})))
          p (pipeline (map stage [f]) :error-handler (fn [_] (swap! e inc)))]
      (binding [*pre-execution-hook* (fn [_] (reset! h0 true))
                *post-execution-hook* (fn [_] (reset! h1 true))]
        (try (run-pipeline-wait p) (catch Exception e))
        (is (= 1 @e))
        (is (and @h0 h1))))))
