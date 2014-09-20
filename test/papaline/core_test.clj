(ns papaline.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan >!! <!! go timeout]]
            [papaline.core :refer :all]))

(deftest test-stage
  (let [f (fn [arg])
        realized-stage (stage f)]
    (is (fn? (.stage-fn realized-stage)))))

(deftest test-pipeline
  (let [f (take 5 (repeat (fn [c] (swap! c inc))))
        stgs (map stage f)
        ppl (pipeline stgs)]
    (is (fn? (.entry-fn ppl)))))

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
        ppl (pipeline [stg0 stg1])]
    (run-pipeline-wait ppl c0)
    (is (= 0 @c0))))

(deftest test-fork-join
  (let [fork-stage (fn [] (fork (take 5 (repeat [1]))))
        join-inc-stage (fn [i] (join (inc i)))
        combine-stage (fn [& l] (reduce + l))]
    (is (= 10 (run-pipeline-wait
               (pipeline [fork-stage join-inc-stage combine-stage]))))
    (is (= [2 2 2 2 2]
           (run-pipeline-wait
            (pipeline [fork-stage join-inc-stage]))))))

(deftest test-args-auto-vec
  (let [fff (fn [] 1)
        ff2 (fn [a] (inc a))]
    (is (= 2 (run-pipeline-wait (pipeline [fff ff2]))))))

(deftest test-error-handler-sync
  (let [e (atom 0)
        f (fn [] (throw (ex-info "expected error.")))
        p (pipeline [f] :error-handler (fn [_ _] (swap! e inc)))]
    (try (run-pipeline-wait p) (catch Exception e))
    (is (= 1 @e))))

(deftest test-error-handler-async
  (let [e (atom 0)
        sync (chan)
        f (fn [] (throw (ex-info "expected error")))
        p (pipeline [f] :error-handler (fn [_ _] (swap! e inc) (>!! sync 1)))]
    (run-pipeline p)
    (<!! sync)
    (is (= 1 @e))))
