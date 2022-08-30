(ns me.tonsky.persistent-sorted-set.bench
  (:require
    [clj-async-profiler.core :as profiler]
    [criterium.core :as criterium]
    [me.tonsky.persistent-sorted-set :as set]))

(def ints-10K
  (vec (shuffle (range 10000))))

(def set-10K
  (into (set/sorted-set) ints-10K))

(def ints-300K
  (vec (shuffle (range 300000))))

(def set-300K
  (into (set/sorted-set) ints-300K))

(defn bench-conj-10K []
  (reduce conj (set/sorted-set) ints-10K))

(defn bench-conj-transient-10K []
  (persistent! (reduce conj! (transient (set/sorted-set)) ints-10K)))

(defn bench-disj-10K []
  (reduce disj set-10K ints-10K))

(defn bench-disj-transient-10K []
  (persistent! (reduce disj! (transient set-10K) ints-10K)))

(defn bench-contains?-10K []
  (doseq [x ints-10K]
    (contains? set-10K x)))

(defn bench-contains-fn-10K []
  (doseq [x ints-10K]
    (set-10K x)))

(defn bench-iterate-300K []
  (let [*res (volatile! 0)]
    (doseq [x set-300K]
      (vswap! *res + x))
    @*res))

(defn bench-reduce-300K []
  (reduce + 0 set-300K))

(defn bench [sym]
  (let [fn            (resolve sym)
        _             (print (format "%-30s" (name sym)))
        _             (flush)
        results       (criterium/quick-benchmark (fn) {})
        [mean & _]    (:mean results)
        [factor unit] (criterium/scale-time mean)]
    (println (criterium/format-value mean factor unit))))

(defn -main []
  (bench `bench-conj-10K)
  (bench `bench-conj-transient-10K)
  (bench `bench-disj-10K)
  (bench `bench-disj-transient-10K)
  (bench `bench-contains?-10K)
  (bench `bench-contains-fn-10K)
  (bench `bench-iterate-300K)
  (bench `bench-reduce-300K))
