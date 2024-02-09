(ns me.tonsky.temporary)

(defn full-range
  [btset ranges]
  (if (:branch? btset)
    (eduction (mapcat full-range) (:children btset))
    (zipmap ranges (repeat (:keys btset)))))

(defn newly-active
  [k ranges-start-order]
  (loop [active []
         [range & ranges :as ordered-ranges] ranges-start-order]
    (if (>= k (-> range :start))
      (recur (conj active range)
             ranges)
      [active ordered-ranges])))

(defn- update-active
  [active-set newly-active finished]
  (as-> active-set active-set
    (reduce disj active-set finished)
    (reduce conj active-set newly-active)))

(defn- fetch-node-stub [node-pointer])

(declare range-query*)
(defn ->child-query
  [node active-ranges inactive-ranges]
  (let [active-ranges-v (volatile! active-ranges)
        inactive-ranges-v (volatile! inactive-ranges)]
    (fn [idx k]
      (let [child-delay (delay (fetch-node-stub (nth (:children node) idx)))
            [newly-active inactive-ranges] (newly-active k @inactive-ranges-v)
            finished-ranges (take-while #(> k (:end %)) @active-ranges-v)]
        (vswap! active-ranges-v update-active newly-active finished-ranges)
        (vreset! inactive-ranges-v inactive-ranges)
        (range-query* child-delay @active-ranges-v @inactive-ranges-v)))))

(defn range-query*
  [btset-node-delay active-ranges inactive-ranges]
  (lazy-seq
   (let [node @btset-node-delay
         child-query (->child-query node active-ranges inactive-ranges)]
     (eduction
      (comp (map-indexed child-query) cat)
      (:keys node)))))

(defn range-query
  [btset ranges]
  (let [ranges-start-order (sort-by :start ranges)
        active-ranges (sorted-set-by :end)]
    (seq
     (range-query* btset ranges-start-order active-ranges))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; BETTER:

(defn process-inactive
  [k covering-ranges inactive-ranges]
  ;; inactive-ranges is ordered by :start
  (loop [covering covering-ranges
         active []
         [range & ranges :as ordered-ranges] inactive-ranges]
    (if (>= k (-> range :start))
      ;; We're in range!
      (if (< k (-> range :end))
        (recur (conj covering range)
               active
               ranges)
        (recur covering
               (conj active range)
               ranges))
      ;; Not in range, so no others will be either:
      [covering active ordered-ranges])))

(declare ->range-query*)
(defn ->child-query
  [node covering-ranges active-ranges inactive-ranges]
  (let [active-ranges-v (volatile! active-ranges)
        inactive-ranges-v (volatile! inactive-ranges)
        f-v (volatile! nil)
        complex-f
        (fn [idx k]
          ;; k is upper-bound for the branch.
          (let [child-delay (delay (fetch-node-stub (nth (:children node) idx)))
                inactive-ranges @inactive-ranges-v
                active-ranges @active-ranges-v
                ;; Linear in non-inactive-ranges:
                [covering newly-active inactive-ranges] (process-inactive k covering-ranges inactive-ranges)
                ;; Linear in no-longer-active ranges:
                finished-ranges (take-while #(< k (:start %)) active-ranges)
                active-ranges (update-active active-ranges newly-active finished-ranges)
                ;; If first active now is covering, all are.
                ;; If not, none are. O(1).

                ]
            (vswap! active-ranges-v update-active newly-active finished-ranges)
            (vreset! inactive-ranges-v inactive-ranges)

            ;; Now, the idea is to recur with
            ;; range-query*
            ;;   - child-delay
            ;;   - covering-ranges
            ;;   - @active-ranges-v
            ;;   - @inactive-ranges-v

            (let [active-not-covering @active-ranges-v ; taking until covering
                  inactive-ranges @active-ranges-v ; dropping until covering
                  inactive-ranges @inactive-ranges-v]

              (eduction
               (comp (mapcat (->range-query* active-not-covering inactive-ranges)))))
            (range-query* child-delay @active-ranges-v @inactive-ranges-v)))]))

(defn ->range-query*
  [covering-ranges active-ranges inactive-ranges]
  (fn range-query [node-delay]
    (let [node @node-delay
          child-query (->child-query node covering-ranges active-ranges inactive-ranges)]
      (if (leaf-node? node)
        ::values-by-range
        (eduction
         (comp (map-indexed child-query) cat)
         (:keys node))))))

(defn transmerge-with
  [xform f init coll]
  (let [init' (transient init)]
    (persistent!
     (transduce
      xform
      (fn [m kv]
        (let [k (key kv) v (val kv)]
          (if-let [v1 (get m k)]
            (assoc! m k (f v1 v))
            (assoc! m k v))))
      init'
      coll))))

(defn range-query
  "Returns map range->results"
  [btset-root ranges]
  (let [covering-ranges []
        active-ranges (sorted-set-by :end)
        inactive-ranges (sort-by :start ranges)]
    (transmerge-with
     (mapcat (->range-query* covering-ranges active-ranges inactive-ranges))
     into
     {}
     [btset-root])))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; OKAY:


(defn- update-active
  [active-set newly-active finished]
  (as-> active-set active-set
    (reduce disj active-set finished)
    (reduce conj active-set newly-active)))

(defn process-inactive
  [k covering-ranges inactive-ranges]
  ;; inactive-ranges is ordered by :start
  (loop [covering covering-ranges
         active []
         [range & ranges :as ordered-ranges] inactive-ranges]
    (if (>= k (-> range :start)) ; We're in range!
      (if (< k (-> range :end))
        (recur (conj covering range)
               active
               ranges)
        (recur covering
               (conj active range)
               ranges))
      ;; Not in range, so no others will be either:
      [covering active ordered-ranges])))

(declare ->range-query*)
(defn ->child-query
  [node covering-ranges active-ranges inactive-ranges]
  (let [active-ranges-v (volatile! active-ranges)
        inactive-ranges-v (volatile! inactive-ranges)]
    (fn [idx k]
          ;; k is inclusive upper-bound for the branch.
          (let [child-delay (delay (fetch-node-stub (nth (:children node) idx)))
                inactive-ranges @inactive-ranges-v
                active-ranges @active-ranges-v
                ;; NOTE:
                ;; If first active now is covering, all are.
                ;; If not, none are. O(1).
                ;;
                ;; Linear in non-inactive-ranges:
                [covering newly-active inactive-ranges] (process-inactive k covering-ranges inactive-ranges)
                ;; Linear in no-longer-active ranges:
                finished-ranges (take-while #(< k (:start %)) active-ranges)


                ]
            (vswap! active-ranges-v update-active newly-active finished-ranges)
            (vreset! inactive-ranges-v inactive-ranges)
            (range-query* child-delay covering @active-ranges-v @inactive-ranges-v)))))

(defn range-query*
  [node-delay covering-ranges active-ranges inactive-ranges]
  (let [node @node-delay
        child-query (->child-query node covering-ranges active-ranges inactive-ranges)]
    (if (leaf-node? node)
      (::values-by-range node (into covering-ranges active-ranges))
      (eduction
       (comp (map-indexed child-query) cat)
       (:keys node)))))

(defn conj-into!
  [transient-m kv]
  (let [k (key kv) v (val kv)]
    (if-let [v1 (get transient-m k)]
      (assoc! transient-m k (into v1 v))
      (assoc! transient-m k v))))

(defn range-query
  "Returns map range->results"
  [btset-root ranges]
  (let [covering-ranges []
        active-ranges (sorted-set-by :end)
        inactive-ranges (sort-by :start ranges)]
    (->>
     (range-query* (delay btset-root) covering-ranges active-ranges inactive-ranges)
     (reduce conj-into! (transient {}))
     (persistent!))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; And here we go:

(defn process-inactive
  [k inactive-ranges]
  ;; inactive-ranges is ordered by :start
  ;; NOTE: newly-active ranges will *never* be covering.
  (loop [active (transient [])
         [range & ranges :as ordered-ranges] inactive-ranges]
    (if (<= k (:start range))
      (recur (conj! active range) ranges)
      ;; Not in range, so no others will be either:
      [(persistent! active) ordered-ranges])))

(defn outfrom
  ([from out]
   (persistent! (reduce disj! (transient from) out)))
  ([from xform out]
   (if (instance? clojure.lang.IEditableCollection from)
     (let [rf (fn
                ([coll] (persistent! coll))
                ([coll v] (disj! coll v)))]
       (transduce xform rf (transient from) out))
     (transduce xform disj from out))))

(defn process-active
  [prev-k k covering-ranges active-ranges:start active-ranges:end newly-active]
  (if (nil? prev-k) ;; no lower bound known, only occurs on left side of tree.
    [covering-ranges active-ranges:start active-ranges:end]
    (let [covering-ranges (transient covering-ranges)
          ;; We will entirely drop these ranges:
          newly-finished (take-while #(<= prev-k (:end %)) active-ranges:end)
          ;; Calculate new covering:
          left-covering (into #{} (take-while #(>= prev-k (:start %))) active-ranges:start)
          ;; In reverse order by end, grab those with an end value greater than our max:
          ;; right-covering (into #{} (take-while #(< k (:end %))) (rseq active-ranges:end))
          ;; newly-covering (set/intersection left-covering right-covering)
          ;; Alternatively, in forward order, REMOVE those with an end value less than our max:
          newly-covering (outfrom left-covering (take-while #(>= k (:end %))) active-ranges:end)
          active-removals (concat newly-finished newly-covering)]
      [(into covering-ranges newly-covering)
       (-> active-ranges:start (outfrom active-removals) (into newly-active))
       (-> active-ranges:end (outfrom active-removals (into newly-active)))])))

(defn- update-active
  [active-set newly-active finished]
  (as-> active-set active-set
    (reduce disj active-set finished)
    (into active-set newly-active)))

(declare range-query*)
(defn ->child-query
  [node covering-ranges inactive-ranges:start inactive-ranges:end inactive-ranges]
  (let [v:active-ranges:start (volatile! active-ranges:start)
        v:active-ranges:end (volatile! active-ranges:end)
        v:inactive-ranges (volatile! inactive-ranges)]
    (fn [idx [prev-k k]]
      ;; k is inclusive upper-bound for the branch.
      (let [child-delay (delay (fetch-node-stub (nth (:children node) idx)))
            inactive-ranges @v:inactive-ranges
            active-ranges:start @v:active-ranges:start
            active-ranges:end @v:active-ranges:end
            ;; Linear in non-inactive-ranges:
            [newly-active inactive-ranges] (process-inactive k inactive-ranges:start inactive-ranges:end)
            ;; ^^ newly active will NEVER be covering.
            [covering-ranges
             active-ranges:start
             active-ranges:end] (process-active prev-k k covering-ranges active-ranges:start active-ranges:end newly-active)]
        (vreset! v:active-ranges:start active-ranges:start)
        (vreset! v:active-ranges:end active-ranges:end)
        (vreset! v:inactive-ranges inactive-ranges)
        ;; Ranges inactive for the parent will be inactive for the children:
        (range-query* child-delay prev-k covering-ranges active-ranges:start active-ranges:end inactive-ranges)))))

(defn range-query*
  [node-delay prev-k covering-ranges active-ranges:start active-ranges:end inactive-ranges]
  (let [node @node-delay
        child-query (->child-query node covering-ranges active-ranges:start active-ranges:end inactive-ranges)]
    (if (leaf-node? node)
      (::values-by-range node (into covering-ranges active-ranges:start))
      (eduction
       (comp (map-indexed child-query) cat)
       (partition 2 1 (cons prev-k (:keys node)))))))

(defn conj-into!
  [transient-m kv]
  (let [k (key kv) v (val kv)]
    (if-let [v1 (get transient-m k)]
      (assoc! transient-m k (into v1 v))
      (assoc! transient-m k v))))

(defn range-query
  "Returns map range->results"
  [btset-root ranges]
  (let [covering-ranges []
        active-ranges:start (sorted-set-by :start)
        active-ranges:end (sorted-set-by :end)
        inactive-ranges (sort-by :start ranges)]
    (->>
     (range-query* (delay btset-root) nil covering-ranges active-ranges:start active-ranges:end inactive-ranges)
     (reduce conj-into! (transient {}))
     (persistent!))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; After some sleep:

(import '[clojure.lang TransformerIterator RT])

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
        (->> (clojure.core/map #(RT/iter %) (cons coll colls))
             (TransformerIterator/createMulti xform)))
       ())))

(defn process-inactive
  [k inactive-ranges:start]
  ;; NOTE: newly-active ranges will *never* be covering.
  (let [active (take-while #(<= k (:start %)) inactive-ranges:start)]
    [active (outfrom inactive-ranges:start active)]))

(defn outfrom
  ([from out]
   (persistent! (reduce disj! (transient from) out)))
  ([from xform out]
   (if (instance? clojure.lang.IEditableCollection from)
     (let [rf (fn
                ([coll] (persistent! coll))
                ([coll v] (disj! coll v)))]
       (transduce xform rf (transient from) out))
     (transduce xform disj from out))))

(defn process-active
  [prev-k k covering-ranges active-ranges:start active-ranges:end newly-active]
  (if (nil? prev-k) ;; no lower bound known, only occurs on left side of tree.
    [covering-ranges active-ranges:start active-ranges:end]
    (let [covering-ranges (transient covering-ranges)
          ;; We will entirely drop these ranges:
          newly-finished (take-while #(<= prev-k (:end %)) active-ranges:end)
          ;; Calculate new covering:
          left-covering (into #{} (take-while #(>= prev-k (:start %))) active-ranges:start)
          ;; In reverse order by end, grab those with an end value greater than our max:
          ;; right-covering (into #{} (take-while #(< k (:end %))) (rseq active-ranges:end))
          ;; newly-covering (set/intersection left-covering right-covering)
          ;; Alternatively, in forward order, REMOVE those with an end value less than our max:
          newly-covering (outfrom left-covering (take-while #(>= k (:end %))) active-ranges:end)
          active-removals (concat newly-finished newly-covering)]
      [(into covering-ranges newly-covering)
       (-> active-ranges:start (outfrom active-removals) (into newly-active))
       (-> active-ranges:end (outfrom active-removals (into newly-active)))])))

(defn- update-active
  [active-set newly-active finished]
  (as-> active-set active-set
    (reduce disj active-set finished)
    (into active-set newly-active)))

(declare range-query*)
(defn ->child-query
  [node covering-ranges inactive-ranges:start]
  ;; What was active-ranges for the parent starts as inactive for the next level.
  (let [v:inactive-ranges:start (volatile! inactive-ranges:start)
        v:active-ranges:start (volatile! (sorted-set-by :start))
        v:active-ranges:end (volatile! (sorted-set-by :end))]
    (fn [idx [prev-k k]]
      ;; k is inclusive upper-bound for the branch.
      (let [child-delay (delay (fetch-node-stub (nth (:children node) idx)))
            inactive-ranges:start @v:inactive-ranges:start
            active-ranges:start @v:active-ranges:start
            active-ranges:end @v:active-ranges:end
            ;; Linear in non-inactive-ranges:
            [newly-active inactive-ranges:start] (process-inactive k inactive-ranges:start)
            ;; ^^ newly active will NEVER be covering.
            [covering-ranges
             active-ranges:start
             active-ranges:end] (process-active prev-k k covering-ranges active-ranges:start active-ranges:end newly-active)]
        ;; Mutate the book-keeping for the next sibling node:
        (vreset! v:active-ranges:start active-ranges:start)
        (vreset! v:active-ranges:end active-ranges:end)
        (vreset! v:inactive-ranges:start inactive-ranges:start)
        ;; Ranges inactive for the parent will be inactive for the children.
        ;; Rnages covering for the parent will be covering for the children.
        (range-query* child-delay prev-k covering-ranges active-ranges:start)))))

(defn range-query*
  [node-delay prev-k covering-ranges ranges:start]
  (let [node @node-delay
        child-query (->child-query node covering-ranges ranges:start)]
    (if (leaf-node? node)
      (::values-by-range node (into covering-ranges ranges:start))
      (sequence
       (comp (map-indexed child-query) cat)
       (partition 2 1 (cons prev-k (:keys node)))))))

(defn conj-into!
  [transient-m kv]
  (let [k (key kv) v (val kv)]
    (if-let [v1 (get transient-m k)]
      (assoc! transient-m k (into v1 v))
      (assoc! transient-m k v))))

(defn range-query
  "Returns map range->results"
  [btset-root ranges]
  (->> (sort-by :start ranges)
       (range-query* (delay btset-root) nil [])
       (reduce conj-into! (transient {}))
       (persistent!)))
