(ns me.tonsky.persistent-sorted-set.test.core
  (:require
   [clojure.data :as data]
   [clojure.test :as t :refer [are deftest is testing]]
   [me.tonsky.batch-slice :as bs]
   [me.tonsky.batch-slice-2 :as bs2]
   [me.tonsky.persistent-sorted-set :as set])
  #?(:clj
      (:import
       [clojure.lang IReduce])))

#?(:clj (set! *warn-on-reflection* true))

(def iters 5)

;; confirm that clj's use of sorted set works as intended.
;; allow for [:foo nil] to glob [:foo *]; data will never be inserted
;; w/ nil, but slice/subseq elements will.

(defn cmp [x y]
  (if (and x y)
    (compare x y)
    0))

(defn cmp-s [[x0 x1] [y0 y1]]
  (let [c0 (cmp x0 y0)
        c1 (cmp x1 y1)]
    (cond
      (= c0 0) c1
      (< c0 0) -1
      (> c0 0)  1)))

(comment
  (let [e0 (set/sorted-set-by cmp-s)
        ds [[:a :b] [:b :x] [:b :q] [:a :d]]
        e1 (reduce conj e0 ds)]
    (bs/batched-range-query e1 [{:start [:a nil] :end [:c nil]}]))

  nil)

(defn batch-slice
  [s from to]
  (let [range {:start from :end to}]
    (get (bs/batched-range-query s [range]) range)))

(defn batch-slice-2
  [s from to]
  (let [range {:start from :end to}]
    (bs2/batched-range-query s [range]) range))

(deftest semantic-test-btset-by
  (let [e0 (set/sorted-set-by cmp-s)
        ds [[:a :b] [:b :x] [:b :q] [:a :d]]
        e1 (reduce conj e0 ds)]
    (is (= (count ds)        (count (seq e1))))
    (is (= (vec (seq e1))    (vec (set/slice e1 [nil nil] [nil nil])))) ; * *
    (is (= [[:a :b] [:a :d]] (vec (set/slice e1 [:a nil]  [:a nil] )))) ; :a *
    (is (= [[:b :q]]         (vec (set/slice e1 [:b :q]   [:b :q]  )))) ; :b :q (specific)
    (is (= [[:a :d] [:b :q]] (vec (set/slice e1 [:a :d]   [:b :q]  )))) ; matching subrange
    (is (= [[:a :d] [:b :q]] (vec (set/slice e1 [:a :c]   [:b :r]  )))) ; non-matching subrange
    (is (= [[:b :x]]         (vec (set/slice e1 [:b :r]   [:c nil] )))) ; non-matching -> out of range
    (is (= []                (vec (set/slice e1 [:c nil]  [:c nil] )))) ; totally out of range
    ))

(defn irange [from to]
  (if (< from to)
    (range from (inc to))
    (range from (dec to) -1)))

(defn random-from-to
  [size]
  (let [from (rand-int (dec size))
        to (+ from (rand-int (- size from)))]
    [from to]))

(def the-test-set
  (into (set/sorted-set) (shuffle (irange 0 100000))))

(def the-ranges (doall (repeatedly 1 #(random-from-to 100000))))

(defn bench-methods
  []
  (let [vanilla-fn (fn [from-tos]
                     (let [r
                           (->> from-tos
                                (map #(into [] (set/slice the-test-set (first %) (second %))))
                                (zipmap from-tos))]
                       r))
        batch-fn (fn [from-tos]
                   (let [ranges (map #(set/->range (first %) (second %))
                                     from-tos)]
                     (set/batch-slice the-test-set ranges)))
        _ (println "-----")
        vanilla-res (time (vanilla-fn the-ranges))
        batch-res (time (batch-fn the-ranges))]
    (time (with-out-str (println vanilla-res)))
    (time (with-out-str (println batch-res)))
    :done
    ))

(deftest multi-select-equality
  (let [size 100000
        s (into (set/sorted-set) (shuffle (irange 0 size)))
        from-tos (doall (repeatedly 100 #(random-from-to size)))
        vanilla-fn (fn [from-tos]
                     (let [r
                           (->> from-tos
                                (map #(into [] (set/slice s (first %) (second %))))
                                (zipmap from-tos))]
                       r))
        batch-fn (fn [from-tos]
                   (let [ranges (map #(set/->range (first %) (second %))
                                     from-tos)]
                     (set/batch-slice s ranges)))
        _ (println "-------")
        vanilla-res (time (vanilla-fn from-tos))
        batch-res (time (batch-fn from-tos))]
    (println)
    (doseq [[from to :as from-to] from-tos]
      (time
       (is (= (get vanilla-res from-to)
              (get batch-res (set/->range from to))))))))

;; The comparator passed to clojure.core/sorted-set-by
;; not ONLY sorts, but ALSO deduplicates when =.
;; This causes bugs in the new code!

;; IDEA:
;; The query engine just needs a range query, not a per-range query
;; So we can collapse the ranges as we go, simply fetching everything in order in the end.
;; A single "covering" range is sufficient to short-circuit all the way to the end.

;; NOTE: it is NOT true that:
;; with collapsing, there never need be more than one active at a time
;;

(comment
  (def test-set (into (set/sorted-set) (shuffle (irange 0 5000))))

  (let [from-tos [[4909 4968] [479 4810] [3548 4881]]
        #_(doall (repeatedly 3 #(random-from-to 5000)))
        vanilla-fn (fn [from-tos]
                     (into (sorted-set) (mapcat #(set/slice test-set (first %) (second %))) from-tos))
        batch-2-fn (fn [from-tos]
                     (let [ranges (map #(set/->range (first %) (second %))
                                       from-tos)]
                       (into (sorted-set)
                             (set/batch-slice-2 test-set ranges))))
        _ (println "-------")
        vanilla-res (time (vanilla-fn from-tos))
        batch-2-res (time (batch-2-fn from-tos))]
    (when-not (= vanilla-res batch-2-res)
      (clojure.pprint/pprint
       {:from-tos from-tos
        :diff
        (data/diff vanilla-res batch-2-res)})))
  ;; [1 16] [11 16] [15 16]
  nil)

(deftest multi-select-equality-2
  (let [size 100000
        s (into (set/sorted-set) (shuffle (irange 0 size)))
        from-tos (doall (repeatedly 1000 #(random-from-to size)))
        vanilla-fn (fn [from-tos]
                     (into (sorted-set) (mapcat #(set/slice s (first %) (second %))) from-tos))
        batch-2-fn (fn [from-tos]
                     (let [ranges (map #(set/->range (first %) (second %))
                                       from-tos)]
                       (into (sorted-set)
                             (set/batch-slice-2 s ranges))))
        _ (println "-------")
        vanilla-res (time (vanilla-fn from-tos))
        batch-2-res (time (batch-2-fn from-tos))]
    (println)
    (is (= vanilla-res
           batch-2-res))
    #_(doseq [[from to :as from-to] from-tos]
        #_(time)
        (is (= (get vanilla-res from-to)
               batch-2-res)
            (pr-str from-to)))))

(deftest multi-select-equality-3
  (let [size 100000
        s (into (set/sorted-set) (shuffle (irange 0 size)))
        from-tos [[12345 90350]] #_[[12345 12350] [15230 15300]]#_(doall (repeatedly 10 #(random-from-to size)))
        vanilla-fn (fn [from-tos]
                     (let [from (reduce min (map first from-tos))
                           to (reduce max (map second from-tos))]
                       (doall (set/slice s from to)))
                     #_(into (sorted-set) (mapcat #(set/slice s (first %) (second %))) from-tos))
        batch-2-fn (fn [from-tos]
                     (let [ranges (map #(set/->range (first %) (second %))
                                       from-tos)]
                       (doall (set/batch-slice-2 s ranges))
                       ))
        _ (println "-------")
        vanilla-res (time (vanilla-fn from-tos))
        batch-2-res (time (batch-2-fn from-tos))]
    (println)
    #_(is (= vanilla-res
             batch-2-res))))

(deftest test-slice
  (dotimes [i 10]
    #_(testing "straight 3 layers"
      (let [s (into (set/sorted-set) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (set/slice s from to))
          nil    nil    (irange 0 5000)
          
          -1     nil    (irange 0 5000)
          0      nil    (irange 0 5000)
          0.5    nil    (irange 1 5000)
          1      nil    (irange 1 5000)
          4999   nil    [4999 5000]
          4999.5 nil    [5000]
          5000   nil    [5000]
          5000.5 nil    nil

          nil    -1     nil
          nil    0      [0]
          nil    0.5    [0]
          nil    1      [0 1]
          nil    4999   (irange 0 4999)
          nil    4999.5 (irange 0 4999)
          nil    5000   (irange 0 5000)
          nil    5001   (irange 0 5000)

          -2     -1     nil
          -1     5001   (irange 0 5000)
          0      5000   (irange 0 5000)
          0.5    4999.5 (irange 1 4999)
          1000   4000   (irange 1000 4000)
          2499.5 2500.5 [2500]
          2500   2500   [2500]
          2500.1 2500.9 nil
          5001   5002   nil)))

    (testing "straight 3 layers"
      (let [s (into (set/sorted-set) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (batch-slice s from to))
          -2     -1     nil
          -1     5001   (irange 0 5000)
          0      5000   (irange 0 5000)
          0.5    4999.5 (irange 1 4999)
          1000   4000   (irange 1000 4000)
          2499.5 2500.5 [2500]
          2500   2500   [2500]
          2500.1 2500.9 nil
          5001   5002   nil
          )))

    #_(testing "straight 1 layer, leaf == root"
      (let [s (into (set/sorted-set) (shuffle (irange 0 10)))]
        (are [from to expected] (= expected (set/slice s from to))
          nil  nil  (irange 0 10)
          
          -1   nil  (irange 0 10)
          0    nil  (irange 0 10)
          0.5  nil  (irange 1 10)
          1    nil  (irange 1 10)
          9    nil  [9 10]
          9.5  nil  [10]
          10   nil  [10]
          10.5 nil  nil
          
          nil -1   nil
          nil 0    [0]
          nil 0.5  [0]
          nil 1    [0 1]
          nil 9    (irange 0 9)
          nil 9.5  (irange 0 9)
          nil 10   (irange 0 10)
          nil 11   (irange 0 10)

          -2   -1  nil
          -1   10  (irange 0 10)
          0    10  (irange 0 10)
          0.5  9.5 (irange 1 9)
          4.5  5.5 [5]
          5    5   [5]
          5.1  5.9 nil
          11   12  nil)))

    (testing "straight 1 layer, leaf == root"
      (let [s (into (set/sorted-set) (shuffle (irange 0 10)))]
        (are [from to expected] (= expected (batch-slice s from to))
          ;; nil  nil  (irange 0 10)
          
          ;; -1   nil  (irange 0 10)
          ;; 0    nil  (irange 0 10)
          ;; 0.5  nil  (irange 1 10)
          ;; 1    nil  (irange 1 10)
          ;; 9    nil  [9 10]
          ;; 9.5  nil  [10]
          ;; 10   nil  [10]
          ;; 10.5 nil  nil
          
          ;; nil -1   nil
          ;; nil 0    [0]
          ;; nil 0.5  [0]
          ;; nil 1    [0 1]
          ;; nil 9    (irange 0 9)
          ;; nil 9.5  (irange 0 9)
          ;; nil 10   (irange 0 10)
          ;; nil 11   (irange 0 10)

          -2   -1  nil
          -1   10  (irange 0 10)
          0    10  (irange 0 10)
          0.5  9.5 (irange 1 9)
          4.5  5.5 [5]
          5    5   [5]
          5.1  5.9 nil
          11   12  nil)))

    #_(testing "reverse 3 layers"
      (let [s (into (set/sorted-set) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (set/rslice s from to))
          nil    nil    (irange 5000 0)
          
          5001   nil    (irange 5000 0)
          5000   nil    (irange 5000 0)
          4999.5 nil    (irange 4999 0)
          4999   nil    (irange 4999 0)
          1      nil    [1 0]
          0.5    nil    [0]
          0      nil    [0]
          -1     nil    nil
          
          nil    5001   nil
          nil    5000   [5000]
          nil    4999.5 [5000]
          nil    4999   [5000 4999]
          nil    1      (irange 5000 1)
          nil    0.5    (irange 5000 1)
          nil    0      (irange 5000 0)
          nil    -1     (irange 5000 0)

          5002   5001   nil
          5001   -1     (irange 5000 0)
          5000   0      (irange 5000 0)
          4000   1000   (irange 4000 1000)
          4999.5 0.5    (irange 4999 1)
          2500.5 2499.5 [2500]
          2500   2500   [2500]
          2500.9 2500.1 nil
          -1     -2     nil)))

    #_(testing "reverse 1 layer, leaf == root"
      (let [s (into (set/sorted-set) (shuffle (irange 0 10)))]
        (are [from to expected] (= expected (set/rslice s from to))
          nil nil (irange 10 0)
          
          11  nil (irange 10 0)
          10  nil (irange 10 0)
          9.5 nil (irange 9 0)
          9   nil (irange 9 0)
          1   nil [1 0]
          0.5 nil [0]
          0   nil [0]
          -1  nil nil
          
          nil 11  nil
          nil 10  [10]
          nil 9.5 [10]
          nil 9   [10 9]
          nil 1   (irange 10 1)
          nil 0.5 (irange 10 1)
          nil 0   (irange 10 0)
          nil -1  (irange 10 0)

          12  11  nil
          11  -1  (irange 10 0)
          10  0   (irange 10 0)
          9.5 0.5 (irange 9 1)
          5.5 4.5 [5]
          5   5   [5]
          5.9 5.1 nil
          -1  -2  nil)))

    #_(testing "seq-rseq equivalence"
      (let [s (into (set/sorted-set) (shuffle (irange 0 5000)))]
        (are [from to] (= (set/slice s from to) (some-> (set/slice s from to) (rseq) (reverse)))
          -1     nil
          0      nil
          2500   nil
          5000   nil
          5001   nil
          
          nil    -1
          nil    0     
          nil    1     
          nil    2500
          nil    5000
          nil    5001  
          
          nil    nil
 
          -1     5001
          0      5000  
          1      4999
          2500   2500
          2500.1 2500.9
          )))

    (testing "seq-rseq equivalence"
      (let [s (into (set/sorted-set) (shuffle (irange 0 5000)))]
        (are [from to] (= (set/slice s from to) (some-> (batch-slice s from to) (rseq) (reverse)))
          ;; -1     nil
          ;; 0      nil
          ;; 2500   nil
          ;; 5000   nil
          ;; 5001   nil
          
          ;; nil    -1
          ;; nil    0     
          ;; nil    1     
          ;; nil    2500
          ;; nil    5000
          ;; nil    5001  
          
          ;; nil    nil
 
          -1     5001
          0      5000  
          1      4999
          2500   2500
          2500.1 2500.9
          )))

    #_(testing "rseq-seq equivalence"
      (let [s (into (set/sorted-set) (shuffle (irange 0 5000)))]
        (are [from to] (= (set/rslice s from to) (some-> (set/rslice s from to) (rseq) (reverse)))
          -1     nil
          0      nil
          2500   nil
          5000   nil
          5001   nil
          
          nil    -1
          nil    0     
          nil    1     
          nil    2500
          nil    5000
          nil    5001  
          
          nil    nil

          5001   -1    
          5000   0       
          4999   1     
          2500   2500  
          2500.9 2500.1)))

    #_(testing "Slice with equal elements"
      (let [cmp10 (fn [a b] (compare (quot a 10) (quot b 10)))
            s10   (reduce #(set/conj %1 %2 compare) (set/sorted-set-by cmp10) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (set/slice s10 from to))
          30 30      (irange 30 39)
          130 4970   (irange 130 4979)
          -100 6000  (irange 0 5000))
        (are [from to expected] (= expected (set/rslice s10 from to))
          30 30      (irange 39 30)
          4970 130   (irange 4979 130)
          6000 -100  (irange 5000 0)))

      (let [cmp100 (fn [a b] (compare (quot a 100) (quot b 100)))
            s100   (reduce #(set/conj %1 %2 compare) (set/sorted-set-by cmp100) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (set/slice s100 from to))
          30  30     (irange 0 99)
          2550 2550  (irange 2500 2599)
          130 4850   (irange 100 4899)
          -100 6000  (irange 0 5000))
        (are [from to expected] (= expected (set/rslice s100 from to))
          30 30      (irange 99 0)
          2550 2550  (irange 2599 2500)
          4850 130   (irange 4899 100)
          6000 -100  (irange 5000 0))))

    (testing "Slice with equal elements"
      (let [cmp10 (fn [a b] (compare (quot a 10) (quot b 10)))
            s10   (reduce #(set/conj %1 %2 compare) (set/sorted-set-by cmp10) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (batch-slice s10 from to))
          30 30      (irange 30 39)
          130 4970   (irange 130 4979)
          -100 6000  (irange 0 5000)))

      (let [cmp100 (fn [a b] (compare (quot a 100) (quot b 100)))
            s100   (reduce #(set/conj %1 %2 compare) (set/sorted-set-by cmp100) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (batch-slice s100 from to))
          ;; 30  30     (irange 0 99)
          2550 2550  (irange 2500 2599)
          ;; 130 4850   (irange 100 4899)
          ;; -100 6000  (irange 0 5000)
          )))))

(defn ireduce
  ([f coll] (#?(:clj .reduce :cljs -reduce) ^IReduce coll f))
  ([f val coll] (#?(:clj .reduce :cljs -reduce) ^IReduce coll f val)))

(defn reduce-chunked [f val coll]
  (if-some [s (seq coll)]
    (if (chunked-seq? s)
      (recur f (#?(:clj .reduce :cljs -reduce) (chunk-first s) f val) (chunk-next s))
      (recur f (f val (first s)) (next s)))
    val))

(deftest test-reduces
  (testing "IReduced"
    (testing "Empty"
      (let [s (set/sorted-set)]
        (is (= 0 (ireduce + s)))
        (is (= 0 (ireduce + 0 s)))))

    (testing "~3 layers"
      (let [s (into (set/sorted-set) (irange 0 5000))]
        (is (= 12502500 (ireduce +   s)))
        (is (= 12502500 (ireduce + 0 s)))
        (is (= 12502500 (ireduce +   (seq s))))
        (is (= 12502500 (ireduce + 0 (seq s))))
        (is (= 7502500  (ireduce +   (set/slice s 1000 4000))))
        (is (= 7502500  (ireduce + 0 (set/slice s 1000 4000))))
        #?@(:clj [(is (= 12502500 (ireduce +   (rseq s))))
                  (is (= 12502500 (ireduce + 0 (rseq s))))
                  (is (= 7502500  (ireduce +   (set/rslice s 4000 1000))))
                  (is (= 7502500  (ireduce + 0 (set/rslice s 4000 1000))))])))

    (testing "~1 layer"
      (let [s (into (set/sorted-set) (irange 0 10))]
        (is (= 55 (ireduce +   s)))
        (is (= 55 (ireduce + 0 s)))
        (is (= 55 (ireduce +   (seq s))))
        (is (= 55 (ireduce + 0 (seq s))))
        (is (= 35 (ireduce +   (set/slice s 2 8))))
        (is (= 35 (ireduce + 0 (set/slice s 2 8))))
        #?@(:clj [(is (= 55 (ireduce +   (rseq s))))
                  (is (= 55 (ireduce + 0 (rseq s))))
                  (is (= 35 (ireduce +   (set/rslice s 8 2))))
                  (is (= 35 (ireduce + 0 (set/rslice s 8 2))))]))))

  (testing "IChunkedSeq"
    (testing "~3 layers"
      (let [s (into (set/sorted-set) (irange 0 5000))]
        (is (= 12502500 (reduce-chunked + 0 s)))
        (is (= 7502500  (reduce-chunked + 0 (set/slice s 1000 4000))))
        (is (= 12502500 (reduce-chunked + 0 (rseq s))))
        (is (= 7502500  (reduce-chunked + 0 (set/rslice s 4000 1000))))))

    (testing "~1 layer"
      (let [s (into (set/sorted-set) (irange 0 10))]
        (is (= 55 (reduce-chunked + 0 s)))
        (is (= 35 (reduce-chunked + 0 (set/slice s 2 8))))
        (is (= 55 (reduce-chunked + 0 (rseq s))))
        (is (= 35 (reduce-chunked + 0 (set/rslice s 8 2))))))))


#?(:clj
    (deftest iter-over-transient
      (let [set (transient (into (set/sorted-set) (range 100)))
            seq (seq set)]
        (conj! set 100)
        (is (thrown-with-msg? Exception #"iterating and mutating" (first seq)))
        (is (thrown-with-msg? Exception #"iterating and mutating" (next seq)))
        (is (thrown-with-msg? Exception #"iterating and mutating" (reduce + seq)))
        (is (thrown-with-msg? Exception #"iterating and mutating" (reduce + 0 seq)))
        (is (thrown-with-msg? Exception #"iterating and mutating" (chunk-first seq)))
        (is (thrown-with-msg? Exception #"iterating and mutating" (chunk-next seq)))
        (is (thrown-with-msg? Exception #"iterating and mutating" (.iterator ^Iterable seq))))))

(deftest seek-for-seq-test
  (let [size 1000
        set (apply set/sorted-set (range size))
        set-seq (seq set)
        set-rseq (rseq set)]
    (testing "simple seek for seq testing"
      (doseq [seek-loc (map #(* 100 %) (range 10))]
        (is (= seek-loc (first (set/seek set-seq seek-loc)))))
      (doseq [seek-loc (map #(* 100 %) (range 10))]
        (is (= seek-loc (first (set/seek set-rseq seek-loc))))))

    (testing "multiple seek testing"
      (is (= 500 (-> set-seq (set/seek 250) (set/seek 500) first)))
      (is (= 500 (-> set-rseq (set/seek 750) (set/seek 500) first))))

    (testing "normal seq behaviour after seek"
      (is (= (range 500 1000) (-> set-seq (set/seek 250) (set/seek 500))))
      (is (= (range 999 499 -1) (-> set-seq (set/seek 250) (set/seek 500) rseq)))
      (is (= (range 500 -1 -1) (-> set-rseq (set/seek 750) (set/seek 500))))
      (is (= (range 0 501) (-> set-rseq (set/seek 750) (set/seek 500) rseq))))

    #?(:clj
       (testing "nil behaviour"
         (is (thrown-with-msg? Exception #"seek can't be called with a nil key!" (set/seek set-seq nil)))))

    (testing "slicing together with seek"
      (is (= (range 5000 7501) (-> (set/slice (apply set/sorted-set (range 10000)) 2500 7500)
                                   (set/seek 5000))))
      (is (= (list 7500) (-> (set/slice (apply set/sorted-set (range 10000)) 2500 7500)
                             (set/seek 5000)
                             (set/seek 7500))))
      (is (= (range 5000 2499 -1) (-> (set/rslice (apply set/sorted-set (range 10000)) 7500 2500)
                                      (set/seek 5000))))
      (is (= (list 2500) (-> (set/rslice (apply set/sorted-set (range 10000)) 7500 2500)
                             (set/seek 5000)
                             (set/seek 2500)))))))
