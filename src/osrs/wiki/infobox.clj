(ns osrs.wiki.infobox
  "DB-only infobox tagging pass.
   Reads :wiki/wikitext and writes :wiki/infobox-name + :wiki/infobox-params-edn."
  (:require
   [clojure.string :as str]
   [osrs.wiki.db :as database]))

(defn first-infobox-template-name
  "Return the first Infobox* template name found, or nil."
  [wikitext]
  (when (string? wikitext)
    (when-let [[_ name] (re-find #"\{\{\s*(Infobox[^|\}\n]+)" wikitext)]
      (str/trim name))))

(defn extract-template-block
  "Extract first occurrence of {{TemplateName ...}} including nested templates."
  [wikitext template-name]
  (when (and (string? wikitext) (not (str/blank? wikitext)))
    (let [needle (str "{{" template-name)
          start  (str/index-of wikitext needle)]
      (when start
        (loop [i start depth 0]
          (cond
            (>= i (count wikitext))
            nil

            (and (< (inc i) (count wikitext))
                 (= \{ (.charAt wikitext i))
                 (= \{ (.charAt wikitext (inc i))))
            (recur (+ i 2) (inc depth))

            (and (< (inc i) (count wikitext))
                 (= \} (.charAt wikitext i))
                 (= \} (.charAt wikitext (inc i))))
            (let [d (dec depth)
                  j (+ i 2)]
              (if (zero? d)
                (subs wikitext start j)
                (recur j d)))

            :else
            (recur (inc i) depth)))))))

(defn parse-template-params
  "Parse |k=v params and support multiline values (continuation lines)."
  [template-block]
  (let [lines (str/split-lines (or template-block ""))]
    (loop [remaining lines
           current-key nil
           current-val ""
           out {}]
      (if (empty? remaining)
        (cond-> out
          current-key (assoc current-key (str/trim current-val)))
        (let [line (first remaining)]
          (if (str/starts-with? line "|")
            (let [[k v] (str/split (subs line 1) #"\s*=\s*" 2)
                  out' (cond-> out
                         current-key (assoc current-key (str/trim current-val)))]
              (recur (rest remaining)
                     (keyword (str/trim k))
                     (or v "")
                     out'))
            (recur (rest remaining)
                   current-key
                   (if current-key
                     (str current-val "\n" line)
                     current-val)
                   out)))))))

(defn parse-first-infobox
  "Return {:infobox-name \"Infobox X\" :params {...}} or nil."
  [wikitext]
  (when-let [infobox-name (first-infobox-template-name wikitext)]
    (when-let [block (extract-template-block wikitext infobox-name)]
      {:infobox-name infobox-name
       :params (parse-template-params block)})))

(defn titles-with-wikitext
  []
  (->> (database/query
        '[:find ?title
          :where
          [?e :wiki/title ?title]
          [?e :wiki/wikitext _]])
       (map first)
       distinct
       sort
       vec))

(defn titles-missing-infobox-tags
  []
  (->> (database/query
        '[:find ?title
          :where
          [?e :wiki/title ?title]
          [?e :wiki/wikitext _]
          (not [?e :wiki/infobox-name _])])
       (map first)
       distinct
       sort
       vec))

(defn store-infobox-tags-for-page!
  [title]
  (let [wikitext (ffirst
                  (database/query
                   '[:find ?wt
                     :in $ ?title
                     :where
                     [?e :wiki/title ?title]
                     [?e :wiki/wikitext ?wt]]
                   title))]
    (if-let [{:keys [infobox-name params]} (parse-first-infobox wikitext)]
      (do
        (database/transact!
         [[:db/add [:wiki/title title] :wiki/infobox-name infobox-name]
          [:db/add [:wiki/title title] :wiki/infobox-params-edn (pr-str params)]])
        {:stored 1 :wiki/title title :infobox-name infobox-name})
      {:stored 0 :wiki/title title :error :no-infobox})))

(defn backfill-infobox-tags!
  "Backfill infobox tags for all pages missing them. Verbose every N."
  ([] (backfill-infobox-tags! {:print-every 1000}))
  ([{:keys [print-every titles]
     :or {print-every 1000}}]
   (let [targets (or titles (titles-missing-infobox-tags))
         total (count targets)]
     (reduce
      (fn [{:keys [index stored skipped] :as acc} title]
        (let [index' (inc index)
              result (store-infobox-tags-for-page! title)
              stored' (+ stored (:stored result))
              skipped' (+ skipped (if (= 1 (:stored result)) 0 1))]
          (when (or (= index' 1) (= index' total) (zero? (mod index' print-every)))
            (println (format "[%d/%d] stored=%d skipped=%d latest=%s (%s)"
                             index' total stored' skipped'
                             (:wiki/title result)
                             (or (:infobox-name result) (:error result)))))
          {:index index' :stored stored' :skipped skipped'}))
      {:index 0 :stored 0 :skipped 0}
      targets))))
