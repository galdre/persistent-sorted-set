(ns me.tonsky.batch-slice-2
  (:refer-clojure :exclude [sequence])
  (:import
   [clojure.lang MapEntry RT TransformerIterator]
   [java.util Comparator]
   [me.tonsky.persistent_sorted_set ANode IStorage PersistentSortedSet]))

(defrecord Range [start end])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utilities (galdre/humilia)

(defn unchunked-iterator-seq
  [^TransformerIterator iter]
  (lazy-seq
   (when ^boolean (.hasNext iter)
     (cons (.next iter) (unchunked-iterator-seq iter)))))

(defn sequence
  "Exactly like clojure.core/sequence, except it does not chunk."
  ([coll]
   (if (seq? coll)
     coll
     (or (seq coll) ())))
  ([xform coll]
   (or (unchunked-iterator-seq
        (TransformerIterator/create xform (RT/iter coll)))
       ()))
  ([xform coll & colls]
   (or (unchunked-iterator-seq
        (->> (map #(RT/iter %) (cons coll colls))
             (TransformerIterator/createMulti xform)))
       ())))

(defn comparator-by
  ^Comparator [^Comparator cmp f1 f2]
  (fn [a b]
    (let [r (.compare cmp (f1 a) (f1 b))]
      (if (zero? r)
        (.compare cmp (f2 a) (f2 b))
        r))))

;;;;;;;;;;;;;;
;; Algorithm:

(defn range-activation-idx
  [^ANode node ^Comparator cmp range]
  (.searchFirst node ^int (:start range) cmp))

(defn range-deactivation-idx
  [^ANode node ^Comparator cmp range]
  (inc (.searchFirst node ^int (:end range) cmp)))

;; Leaves:

(defn leaf-covering-query ; It's NOT coming from here.
  [node-delay]
  (let [node @node-delay
        len (.-_len node)]
    (into [] (take len) (.-_keys ^ANode node))))

(defn leaf-solo-query
  [node-delay cmp range]
  (let [^ANode node @node-delay
        start-idx (.searchFirst node ^int (:start range) cmp)
        stop-idx (inc (.searchLast node ^int (:end range) cmp))
        ks (.-_keys node)]
    (loop [result (transient [])
           idx start-idx]
      (if (= idx stop-idx)
        (persistent! result)
        (recur (conj! result (aget ks idx)) (unchecked-inc idx))))))

(defn leaf-ranges-query
  [node-delay cmp ranges:start]
  (cond (empty? ranges:start)
        nil
        (== 1 (count ranges:start))
        (leaf-solo-query node-delay cmp (first ranges:start))
        :else
        (let [node @node-delay
              len (.-_len node)
              ks (.-_keys ^ANode node)
              range (first ranges:start)]
          (loop [results (transient [])
                 ranges (next ranges:start)
                 idx (.searchFirst node ^int (:start range) cmp)
                 stop-idx (inc (.searchLast node ^int (:end range) cmp))
                 next-start-idx (.searchFirst node ^int (:start (first ranges)) cmp)]
            (cond
              (== idx len) ; we hit the end
              (persistent! results)

              (= idx next-start-idx) ; new range activated, update stop-idx
              (let [new-range (first ranges)
                    ranges (next ranges)
                    stop-idx' (inc (.searchLast node ^int (:end new-range) cmp))]
                (recur results
                       ranges
                       idx
                       (max stop-idx stop-idx')
                       (when-let [next-range (first ranges)]
                         (.searchFirst node ^int (:start next-range) cmp))))

              (some->> stop-idx (== idx)) ; we need to stop or skip
              (if next-start-idx
                (recur results
                       ranges
                       next-start-idx
                       stop-idx
                       next-start-idx)
                (persistent! results))

              :else ; add to results
              (recur (conj! results (aget ks ^int idx))
                     ranges
                     (unchecked-inc idx)
                     stop-idx
                     next-start-idx))))))

;; Branches:

(defn fetch-node
  [^ANode node ^IStorage storage idx]
  (.child node storage ^int idx))

(defn is-not-covering?
  [parent-idx idx len range]
  (or (= 0 idx parent-idx)
      (= idx @(:activation-idx (meta range)))
      (= idx (dec @(:deactivation-idx (meta range))))
      (= idx (dec len))))

(defn tag-range
  [node cmp range]
  (with-meta
    range
    {:activation-idx (delay (range-activation-idx node cmp range))
     :deactivation-idx (delay (range-deactivation-idx node cmp range))}))

(defn branch-covering-query
  [node-delay ^IStorage storage]
  (let [node @node-delay
        next-is-leaf (== 1 (.level node))
        ^int len (.-_len node)]
    (loop [idx 0
           subresults (transient [])]
      (if (== idx len)
        (persistent! subresults)
        (let [child-delay (delay (fetch-node node storage idx))
              new-result (if next-is-leaf
                           (lazy-seq (leaf-covering-query child-delay))
                           (lazy-seq (branch-covering-query child-delay storage)))]
          (recur (unchecked-inc idx)
                 (conj! subresults new-result)))))))

(defn branch-solo-query
  [node-delay ^IStorage storage parent-idx ^Comparator cmp range]
  (let [^ANode node @node-delay
        range (tag-range node cmp range)
        next-is-leaf (== 1 (.level node))
        len (.-_len node)
        end-idx (range-deactivation-idx node cmp range)]
    (loop [idx (range-activation-idx node cmp range)
           subresults (transient [])]
      (if (or (= idx len) (= idx end-idx))
        (persistent! subresults)
        (let [child-delay (delay (fetch-node node storage idx))
              new-result (if (is-not-covering? parent-idx idx len range)
                           (if next-is-leaf
                             (lazy-seq
                              (leaf-solo-query child-delay cmp range))
                             (lazy-seq
                              (branch-solo-query child-delay storage idx cmp range)))
                           (if next-is-leaf
                             (lazy-seq (leaf-covering-query child-delay))
                             (lazy-seq (branch-covering-query child-delay storage))))]
          (recur (inc idx) (conj! subresults new-result)))))))

(defn branch-ranges-query
  [node-delay
   ^IStorage storage
   parent-idx
   ^Comparator cmp
   ^Comparator start-cmp
   ^Comparator end-cmp
   inactive-ranges:start]
  ;; Until we get to leaves, we have to track each range individually
  ;; to propagate correctly. But an optimization we COULD do is collapse
  ;; overlapping ranges at the very start.
  (if (== 1 (count inactive-ranges:start))
    ;; Go to faster branch:
    (branch-solo-query node-delay storage parent-idx cmp (first inactive-ranges:start))
    ;; Do the slower thing:
    (let [^ANode node @node-delay
          next-is-leaf (== 1 (.level node))
          shortcircuit-idx (.-_len node)
          inactive-ranges:start
          (into []
                (map (fn [range] (tag-range node cmp range)))
                inactive-ranges:start)]
      (loop [inactive-ranges:start inactive-ranges:start
             active-ranges:start (sorted-set-by start-cmp)
             active-ranges:end (sorted-set-by end-cmp) ;; sorted-group-by end-idx
             idx 0
             next-activation-idx (some-> inactive-ranges:start first meta :activation-idx deref)
             next-deactivation-idx nil
             subresults (transient [])]
        (cond (= idx shortcircuit-idx)
              (persistent! subresults)

              (some->> next-deactivation-idx (= idx))
              (let [newly-inactive (first active-ranges:end)
                    active-ranges:end (disj active-ranges:end newly-inactive)]
                (recur inactive-ranges:start
                       (disj active-ranges:start newly-inactive)
                       active-ranges:end
                       idx
                       next-activation-idx
                       (some-> active-ranges:end first meta :deactivation-idx deref)
                       subresults))

              (= idx next-activation-idx)
              (let [newly-active (first inactive-ranges:start)
                    inactive-ranges:start (subvec inactive-ranges:start 1)]
                (recur inactive-ranges:start
                       (conj active-ranges:start newly-active)
                       (conj active-ranges:end newly-active)
                       idx
                       (some-> inactive-ranges:start first meta :activation-idx deref)
                       (if next-deactivation-idx
                         (min next-deactivation-idx
                              @(:deactivation-idx (meta newly-active)))
                         @(:deactivation-idx (meta newly-active)))
                       subresults))
              (empty? active-ranges:start)
              (if (nil? next-activation-idx)
                (persistent! subresults)
                (recur inactive-ranges:start
                       active-ranges:start
                       active-ranges:end
                       next-activation-idx
                       next-activation-idx
                       next-deactivation-idx
                       subresults))
              :else
              (let [child-delay (delay (fetch-node node storage idx))
                    covering (some (complement #(is-not-covering? parent-idx idx shortcircuit-idx %))
                                   active-ranges:start)
                    new-result
                    (if next-is-leaf
                      (if covering
                        (lazy-seq
                         (leaf-covering-query child-delay))
                        (lazy-seq
                         (leaf-ranges-query child-delay
                                            cmp
                                            active-ranges:start)))
                      (if covering
                        (->> (branch-covering-query child-delay storage)
                             (sequence cat))
                        (->> (branch-ranges-query child-delay storage idx cmp start-cmp end-cmp
                                           active-ranges:start)
                             (sequence cat))))]
                (recur inactive-ranges:start
                       active-ranges:start
                       active-ranges:end
                       (inc idx)
                       next-activation-idx
                       next-deactivation-idx
                       (conj! subresults new-result))))))))

(defn batched-range-query
  "Returns map range->results"
  [^PersistentSortedSet btset ranges]
  (let [btset-root (.root btset)
        cmp (.comparator btset)
        storage (.-_storage btset)
        is-a-leaf (== 0 (.level btset-root))]
    (if (== 1 (count ranges))
      ;; Single Range:
      (if is-a-leaf
        (leaf-solo-query (delay btset-root) cmp (first ranges))
        (->> (branch-solo-query (delay btset-root) storage 0 cmp (first ranges))
             (into [] cat)))
      ;; Multiple Ranges:
      (let [start-cmp (comparator-by cmp :start :end)
            end-cmp (comparator-by cmp :end :start)
            ranges:start (into [] (sort start-cmp ranges))]
        (if is-a-leaf
          (leaf-ranges-query (delay btset-root) cmp ranges:start)
          (->> (branch-ranges-query (delay btset-root) storage 0 cmp start-cmp end-cmp ranges:start)
               (into [] cat)))))))
