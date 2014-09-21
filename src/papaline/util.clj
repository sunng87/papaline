(ns papaline.util)

(defmacro defprotocol+ [name & funcs]
  (let [vararg-sym (symbol "&")
        normalized-func-specs (map #(let [[n a] %]
                                      (if (.contains a vararg-sym)
                                        [(symbol (str n "*"))
                                         (vec (remove (fn [_a]
                                                        (= _a vararg-sym)) a))
                                         [n a]]
                                        [n a]))
                                   funcs)
        vararg-funcs (filter #(> (count %) 2) normalized-func-specs)]
    `(do
       (defprotocol ~name
         ~@(map #(list (first %) (second %)) normalized-func-specs))
       ~@(map #(let [[prot-func prot-args original-spec] %
                     [n a] original-spec]
                 (list 'defn n a
                       (apply list prot-func prot-args))) vararg-funcs))))
