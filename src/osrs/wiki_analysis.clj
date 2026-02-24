(ns osrs.wiki-analysis
  "DB-only analysis helpers over the local wiki mirror.

  Depends on osrs.wiki-extract for:
    - open-db!, q, transact!
    - template parsing helpers (template, etc.)"
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [osrs.wiki-extract :as wx]))

;; ------------------------------------------------------------
;; Safe parsing helpers
;; ------------------------------------------------------------

(defn safe-read-edn
  [s]
  (try (edn/read-string s)
       (catch Exception _ nil)))

(defn parse-bool [s]
  (when (string? s)
    (case (str/lower-case (str/trim s))
      ("yes" "true" "1") true
      ("no" "false" "0") false
      nil)))

(defn parse-long [s]
  (when (string? s)
    (let [t (-> s str/trim (str/replace #"," "") (str/replace #"^\+" ""))]
      (try (Long/parseLong t)
           (catch Exception _ nil)))))

(defn parse-double [s]
  (when (string? s)
    (let [t (-> s str/trim (str/replace #"," "") (str/replace #"^\+" ""))]
      (try (Double/parseDouble t)
           (catch Exception _ nil)))))

;; ------------------------------------------------------------
;; Infobox inspection (DB-only)
;; ------------------------------------------------------------

(defn top-infobox-keys
  "Return {:sampled N :parsed M :bad K :top [[k n] ...]} for an infobox name."
  ([infobox-name] (top-infobox-keys infobox-name 5000 50))
  ([infobox-name sample-n top-n]
   (let [rows   (wx/q '[:find ?p
                        :in $ ?name
                        :where
                        [?e :wiki/infobox-name ?name]
                        [?e :wiki/infobox-params-edn ?p]]
                      infobox-name)
         sample (take sample-n rows)
         maps   (->> sample
                     (map first)
                     (map safe-read-edn)
                     (remove nil?))
         freqs  (->> maps (mapcat keys) frequencies)]
     {:sampled (count sample)
      :parsed  (count maps)
      :bad     (- (count sample) (count maps))
      :top     (->> freqs
                    (sort-by val >)
                    (take top-n)
                    (mapv (fn [[k v]] [k v])))})))

(defn top-infobox-item-keys
  ([] (top-infobox-keys "Infobox Item"))
  ([sample-n top-n] (top-infobox-keys "Infobox Item" sample-n top-n)))

;; ------------------------------------------------------------
;; Item normalization backfill (DB-only)
;; ------------------------------------------------------------

(defn infobox-item-keys []
  (->> (top-infobox-item-keys 5000 60)
       :top
       (map first)
       vec))

(defn normalize-item-entity
  "Turn parsed Infobox Item params into a normalized entity map.
  Stores both typed fields and a full infobox blob."
  [title params infobox-keys]
  (wx/remove-nils
   {:wiki/title        title
    :osrs/item/infobox (pr-str (select-keys params infobox-keys))

    :osrs/item-id      (parse-long (:id params))
    :osrs/name         (:name params)
    :osrs/examine      (:examine params)
    :osrs/value        (parse-long (:value params))
    :osrs/weight       (parse-double (:weight params))

    :osrs/members?     (parse-bool (:members params))
    :osrs/tradeable?   (parse-bool (:tradeable params))
    :osrs/stackable?   (parse-bool (:stackable params))
    :osrs/equipable?   (parse-bool (:equipable params))
    :osrs/noteable?    (parse-bool (:noteable params))
    :osrs/placeholder? (parse-bool (:placeholder params))
    :osrs/quest?       (parse-bool (:quest params))
    :osrs/exchange?    (parse-bool (:exchange params))
    :osrs/alchable?    (parse-bool (:alchable params))
    :osrs/bankable?    (parse-bool (:bankable params))

    :osrs/options      (:options params)
    :osrs/update       (:update params)
    :osrs/release      (:release params)
    :osrs/image        (:image params)
    :osrs/destroy      (:destroy params)
    :osrs/leagueRegion (:leagueRegion params)}))

(defn backfill-items!
  "Backfill normalized item datoms from stored Infobox Item params.
  Prints every `print-every` stored entities.

  Defensive against odd/partial rows coming back from datalevin spill vectors."
  ([] (backfill-items! 5000 2000))
  ([sample-n print-every]
   (wx/open-db!)
   (let [infobox-keys (->> (top-infobox-item-keys sample-n 80) :top (map first) vec)
         rows-raw (wx/q '[:find ?t ?p
                          :where
                          [?e :wiki/infobox-name "Infobox Item"]
                          [?e :wiki/title ?t]
                          [?e :wiki/infobox-params-edn ?p]])
         ;; normalize rows into plain vectors and drop anything weird
         rows (->> rows-raw
                   (map (fn [r] (when (and (sequential? r) (= 2 (count r))) (vec r))))
                   (remove nil?)
                   vec)
         total (count rows)]
     (reduce
      (fn [{:keys [i stored skipped bad] :as acc} [t p]]
        (let [params (safe-read-edn p)
              ent    (when (map? params) (normalize-item-entity t params infobox-keys))
              i'     (inc i)]
          (cond
            (nil? params)
            {:i i' :stored stored :skipped skipped :bad (inc bad)}

            (or (nil? ent) (nil? (:osrs/item-id ent)))
            {:i i' :stored stored :skipped (inc skipped) :bad bad}

            :else
            (do
              (wx/transact! [ent])
              (let [stored' (inc stored)]
                (when (zero? (mod stored' print-every))
                  (println (format "[%d/%d] stored=%d latest=%s"
                                   i' total stored' t)))
                {:i i' :stored stored' :skipped skipped :bad bad})))))
      {:i 0 :stored 0 :skipped 0 :bad 0}
      rows))))

(defn base-key
  "Turn :name12 -> :name, :bucketname3 -> :bucketname, leave :name as-is."
  [k]
  (-> k name (str/replace #"\d+$" "") keyword))

(defn db-wikitext
  [title]
  (ffirst
   (osrs.wiki-ingest/q
    '[:find ?wt
      :in $ ?t
      :where
      [?e :wiki/title ?t]
      [?e :wiki/wikitext ?wt]]
    title)))




(defn extract-template-block
  "Return the first {{Template ...}} block (raw string), or nil."
  [wikitext template-name]
  (when (and (string? wikitext) (not (str/blank? wikitext)))
    (let [needle (str "{{" template-name)
          start  (str/index-of wikitext needle)]
      (when start
        (loop [i start depth 0]
          (cond
            (>= i (count wikitext)) nil

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
  "Parse lines like: |key = value into {:key \"value\"}."
  [template-block]
  (->> (str/split-lines (or template-block ""))
       (keep (fn [line]
               (when (str/starts-with? line "|")
                 (let [[k v] (str/split (subs line 1) #"\s*=\s*" 2)]
                   (when (and k v)
                     [(keyword (str/trim k)) (str/trim v)])))))
       (into {})))



(defn all-item-infobox-keys
  []
  (let [rows
        (osrs.wiki-ingest/q
         '[:find ?t
           :where
           [?e :wiki/title ?t]
           [?e :wiki/wikitext _]])]
    (->> rows
         (map first)
         (map item-infobox)
         (remove nil?)
         (mapcat keys)
         distinct
         sort
         vec)))

(defn item-infobox
  "Return parsed Infobox Item map for a title from the local DB."
  [title]
  (let [wt (db-wikitext title)
        block (extract-template-block wt "Infobox Item")]
    (when block
      (parse-template-params block))))


(def smushed-item-keys
  (->> (all-item-infobox-keys)
       (map base-key)
       distinct
       sort
       vec))


(def key-variants
  (->> (all-item-infobox-keys)
       (map (fn [k] [(base-key k) k]))
       (group-by first)))

(->> key-variants
     (map (fn [[bk pairs]] [bk (count pairs)]))
     (sort-by second >)
     (take 30))

(defn items-with-removal
  []
  (->> (osrs.wiki-ingest/q
        '[:find ?t
          :where
          [?e :wiki/title ?t]
          [?e :wiki/wikitext _]])
       (map first)
       (map (fn [t] [t (item-infobox t)]))
       (filter (fn [[_ ib]] (and ib (:removal ib))))
       (map first)
       vec))

(items-with-removal)
