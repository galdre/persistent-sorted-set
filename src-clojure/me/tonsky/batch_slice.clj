(ns me.tonsky.batch-slice
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

(defn outfrom
  ([from out]
   (if (instance? clojure.lang.IEditableCollection from)
     (persistent! (reduce disj! (transient from) out))
     (reduce disj from out)))
  ([from xform out]
   (if (instance? clojure.lang.IEditableCollection from)
     (let [rf (fn
                ([coll] (persistent! coll))
                ([coll v] (disj! coll v)))]
       (transduce xform rf (transient from) out))
     (transduce xform disj from out))))

(defn map-vals
  "Maps a function across the vals of a MapEntry collection. Returns
  a sequence. If you want a new map efficiently constructed, use
  (into {} (map-vals f) c)."
  ([f]
   (map (fn [entry-map]
          (MapEntry/create (key entry-map)
                           (-> entry-map val f)))))
  ([f k->v]
   (map (fn [entry-map]
          (MapEntry/create (key entry-map)
                           (-> entry-map val f)))
        k->v)))

(defn comparator-by
  ^Comparator [^Comparator cmp f]
  (fn [a b] (.compare cmp (f a) (f b))))

(defmacro <=*
  [cmp x y]
  `(<= (.compare ~(with-meta cmp {:tag Comparator}) ~x ~y) 0))

(defmacro <*
  [cmp x y]
  `(< (.compare ~(with-meta cmp {:tag Comparator}) ~x ~y) 0))

(defmacro >=*
  [cmp x y]
  `(>= (.compare ~(with-meta cmp {:tag Comparator}) ~x ~y) 0))

(defmacro >*
  [cmp x y]
  `(> (.compare ~(with-meta cmp {:tag Comparator}) ~x ~y) 0))

;;;;;;;;;;;;;;
;; Algorithm:

;; Leaves:

(defn collect-leaf-values
  [cmp end-cmp ks ranges:start]
  ;; TODO: employ binary search on first range to find idx for starting loop on keys
  (loop [active-ranges:end (sorted-set-by end-cmp)
         ranges:start ranges:start
         results {}
         [k & ks] ks]
    (if-not (and k (or (seq active-ranges:end) (seq ranges:start)))
      ;; Short-circuit if we ran out of keys or out of ranges
      (into {} (map-vals persistent!) results)
      (let [newly-active (take-while #(and (<=* cmp (:start %) k)
                                           (<=* cmp k (:end %)))
                                     ranges:start)
            newly-finished (take-while #(<* cmp (:end %) k) active-ranges:end)
            active-ranges:end (-> active-ranges:end (outfrom newly-finished) (into newly-active))
            ranges:start (outfrom ranges:start newly-active)
            results' (persistent!
                      (reduce
                       (fn [results' range]
                         (if-let [prev-results (get results range)]
                           (assoc! results' range (conj! prev-results k))
                           (assoc! results' range (conj! (transient []) k))))
                       (transient results)
                       active-ranges:end))]
        (recur active-ranges:end ranges:start results' ks)))))

(defn leaf-range-query
  "Returns sequence of MapEntries: <range, partial-result>."
  [node-delay cmp end-cmp covering-ranges other-ranges:start]
  (when-not (and (empty? covering-ranges) (empty? other-ranges:start))
    (let [ks (into [] (filter some?) (.-_keys ^ANode @node-delay))
          ;; The easy ones:
          covering-results (map #(MapEntry/create % ks) covering-ranges)
          ;; Collect the rest:
          searched-results (some->> other-ranges:start not-empty (collect-leaf-values cmp end-cmp ks))]
      (concat covering-results searched-results))))

;; Branches:

(defn process-inactive
  [cmp k inactive-ranges:start]
  (let [newly-active (take-while #(<=* cmp (:start %) k) inactive-ranges:start)]
    [newly-active (outfrom inactive-ranges:start newly-active)]))

(defn ->newly-covering
  [cmp prev-k k active-ranges:start active-ranges:end]
  (let [left-covering (into #{} (take-while #(<=* cmp (:start %) prev-k)) active-ranges:start)
        ;; Naively: in reverse order by end, grab those with an end value greater than our max:
        ;; right-covering (into #{} (take-while #(> (:end %) k)) (rseq active-ranges:end))
        ;; newly-covering (set/intersection left-covering right-covering)
        ]
    ;; Alternatively, in forward order, REMOVE those with an end value less than our max:
    (outfrom left-covering (take-while #(<=* cmp (:end %) k)) active-ranges:end)))

(defn process-active
  [cmp prev-k k covering-ranges active-ranges:start active-ranges:end newly-active]
  ;; Ignore newly-active here because they cannot be covering.
  (if (nil? prev-k)
    ;; no lower bound known.
    ;; This means we can't know which ranges are "finished" or "covering"
    ;; Only occurs on left-most branches.
    [covering-ranges (into active-ranges:start newly-active) (into active-ranges:end newly-active)]
    (let [;; We will entirely drop these ranges:
          newly-finished (take-while #(<* cmp (:end %) prev-k) active-ranges:end)
          ;; Note: ^^^^ not <=* because comparator might establish funky equivalence classes
          ;; and this library lets you conj with a distinct tie-breaking comparator.
          ;; Calculate new covering ranges:
          newly-covering (->newly-covering cmp prev-k k active-ranges:start active-ranges:end)
          active-removals newly-finished #_(concat newly-finished newly-covering)]
      [newly-covering
       (-> active-ranges:start (outfrom active-removals) (into newly-active))
       (-> active-ranges:end (outfrom active-removals) (into newly-active))])))

(defn fetch-node
  [^ANode node ^IStorage storage idx]
  (.child node storage ^int idx))

(declare range-query*)
(defn ->child-query
  [^ANode node storage cmp start-cmp end-cmp covering-ranges inactive-ranges:start]
  ;; What were active-ranges for the parent start as inactive for the children.
  (let [v:inactive-ranges:start (volatile! inactive-ranges:start)
        v:active-ranges:start (volatile! (sorted-set-by start-cmp))
        v:active-ranges:end (volatile! (sorted-set-by end-cmp))]
    (fn [idx [prev-k k]]
      ;; k is inclusive upper-bound for the branch.
      (when k;; The library pads the end of the keys array with `nil`s.
        (let [inactive-ranges:start @v:inactive-ranges:start
              active-ranges:start @v:active-ranges:start
              active-ranges:end @v:active-ranges:end]
          (when (or (seq inactive-ranges:start) (seq covering-ranges) (seq active-ranges:start))
            (let [;; Linear in non-inactive-ranges:
                  [newly-active inactive-ranges:start] (process-inactive cmp k inactive-ranges:start)
                  ;; ^^ newly active ranges will NEVER be covering.
                  [newly-covering ; for children, but not necessarily for siblings.
                   active-ranges:start
                   active-ranges:end] (process-active cmp prev-k k covering-ranges active-ranges:start active-ranges:end newly-active)
                  child-delay (delay (fetch-node node storage idx))]
              ;; Mutate the book-keeping for the next sibling node:
              (vreset! v:active-ranges:start active-ranges:start)
              (vreset! v:active-ranges:end active-ranges:end)
              (vreset! v:inactive-ranges:start inactive-ranges:start)
              ;; RECURSE:
              ;; Ranges inactive for the parent will be inactive for the children.
              ;; Ranges covering for the parent will be covering for the children.
              (if (= 1 (.level ^ANode node))
                ;; the child is a leaf:
                (leaf-range-query child-delay cmp end-cmp
                                  (into covering-ranges newly-covering)
                                  (outfrom active-ranges:start newly-covering))
                (range-query* child-delay storage cmp start-cmp end-cmp prev-k
                              (into covering-ranges newly-covering)
                              (outfrom active-ranges:start newly-covering))))))))))

(defn range-query*
  "Returns sequence of MapEntries: <range, partial-result>."
  [node-delay storage cmp start-cmp end-cmp prev-k covering-ranges ranges:start]
  (when-not (and (empty? ranges:start) (empty? covering-ranges))
    (let [node @node-delay
          child-query (->child-query node storage cmp start-cmp end-cmp covering-ranges ranges:start)]
      (sequence
       (comp (map-indexed child-query) cat)
       (partition 2 1 (cons prev-k (filter some? (.-_keys ^ANode node))))))))

(defn process-result-stream
  [result-stream]
  (let [process-result
        (fn [result' kv]
          (let [range (key kv)
                partial-results (val kv)]
            (update result'
                    range
                    (fn [temp-results]
                      (if (some? temp-results)
                        (reduce conj! temp-results partial-results)
                        (transient partial-results))))))]
    (->> result-stream
         (reduce process-result {})
         (into {} (map-vals #(some-> % persistent!))))))

(defn batched-range-query
  "Returns map range->results"
  [^PersistentSortedSet btset ranges]
  (let [btset-root (.root btset)
        cmp (.comparator btset)
        start-cmp (comparator-by cmp :start)
        end-cmp (comparator-by cmp :end)
        storage (.-_storage btset)
        ranges:start (into (sorted-set-by start-cmp) ranges)
        result-stream (if (= 0 (.level btset-root))
                        (leaf-range-query (delay btset-root) cmp end-cmp nil ranges:start)
                        (range-query* (delay btset-root) storage cmp start-cmp end-cmp nil [] ranges:start))]
    (process-result-stream result-stream)))
