(ns osrs.wiki.extract.quest
  "Quest extraction from DB-mirrored wikitext.

  Requires:
    - Wiki pages mirrored with :wiki/wikitext (osrs.wiki.mirror)
    - Infobox tags present so we can recognize item pages (osrs.wiki.infobox)

  Extracts from {{Quest details ...}}:
    - requirements -> :quest/prerequisite-quest (many)
    - items       -> :quest/required-item (many) + :quest/required-link (many)
    - recommended -> :quest/recommended-item (many) + :quest/recommended-link (many)"
  (:require
   [clojure.string :as str]
   [osrs.wiki.db :as database]))

;; ------------------------------------------------------------
;; DB helpers
;; ------------------------------------------------------------

(defn fetch-page-wikitext
  [title]
  (ffirst
   (database/query
    '[:find ?wt
      :in $ ?t
      :where
      [?e :wiki/title ?t]
      [?e :wiki/wikitext ?wt]]
    title)))

(defn quest-title?
  "True if page is tagged as Infobox Quest."
  [title]
  (boolean
   (ffirst
    (database/query
     '[:find ?e
       :in $ ?t
       :where
       [?e :wiki/title ?t]
       [?e :wiki/infobox-name "Infobox Quest"]]
     title))))

(defn item-title?
  "True if page is tagged as Infobox Item."
  [title]
  (boolean
   (ffirst
    (database/query
     '[:find ?e
       :in $ ?t
       :where
       [?e :wiki/title ?t]
       [?e :wiki/infobox-name "Infobox Item"]]
     title))))

(defn all-quest-titles
  []
  (->> (database/query
        '[:find ?t
          :where
          [?e :wiki/title ?t]
          [?e :wiki/infobox-name "Infobox Quest"]])
       (map first)
       distinct
       sort
       vec))

;; ------------------------------------------------------------
;; Build item-title index once (case-insensitive)
;; ------------------------------------------------------------

(defonce item-title-index-atom (atom nil))

(defn refresh-item-title-index!
  []
  (let [titles (->> (database/query
                     '[:find ?t
                       :where
                       [?e :wiki/title ?t]
                       [?e :wiki/infobox-name "Infobox Item"]])
                    (map first)
                    distinct
                    vec)
        index (into {} (map (fn [t] [(str/lower-case t) t])) titles)]
    (reset! item-title-index-atom index)
    {:items (count titles)}))

(defn ensure-item-title-index!
  []
  (when-not @item-title-index-atom
    (refresh-item-title-index!))
  :ok)

(defn canonical-item-title
  "Return canonical item title for a link target, or nil."
  [link-target]
  (when (string? link-target)
    (get @item-title-index-atom (str/lower-case (str/trim link-target)))))

;; ------------------------------------------------------------
;; Wikitext template extraction (multiline)
;; ------------------------------------------------------------

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
  "Parse |k=v params supporting multiline values."
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

(defn parse-template
  [wikitext template-name]
  (when-let [block (extract-template-block wikitext template-name)]
    (parse-template-params block)))

(defn wikilinks-in-text
  "Extract [[Target]] / [[Target|...]] targets."
  [text]
  (when (string? text)
    (->> (re-seq #"\[\[([^\]|#]+)" text)
         (map second)
         (map str/trim)
         (remove str/blank?)
         (remove (fn [t]
                   (or (str/starts-with? t "File:")
                       (str/starts-with? t "Category:")
                       (str/starts-with? t "Help:")
                       (str/starts-with? t "Special:")
                       (str/starts-with? t "Template:")
                       (str/starts-with? t "Module:")
                       (str/starts-with? t "RuneScape:")
                       (str/starts-with? t "Update:")
                       (str/starts-with? t "Poll:"))))
         distinct
         vec)))

;; ------------------------------------------------------------
;; Storage helpers for cardinality-many
;; ------------------------------------------------------------

(defn replace-many-attribute!
  "Replace all values of a cardinality-many attribute on [:wiki/title title]."
  [title attribute new-values]
  (let [old-values (map first
                        (database/query
                         '[:find ?v
                           :in $ ?t ?a
                           :where
                           [?e :wiki/title ?t]
                           [?e ?a ?v]]
                         title attribute))
        new-values (->> new-values
                        (filter string?)
                        (map str/trim)
                        (remove str/blank?)
                        distinct
                        vec)
        retracts (mapv (fn [v] [:db/retract [:wiki/title title] attribute v]) old-values)
        adds (mapv (fn [v] [:db/add [:wiki/title title] attribute v]) new-values)]
    (database/transact! (into [] (concat retracts adds)))
    {:title title :attribute attribute :old (count old-values) :new (count new-values)}))

;; ------------------------------------------------------------
;; Main extraction
;; ------------------------------------------------------------

(defn fetch-quest-details
  "Parse {{Quest details ...}} from quest page wikitext."
  [quest-title]
  (let [wikitext (fetch-page-wikitext quest-title)]
    (when (string? wikitext)
      (parse-template wikitext "Quest details"))))

(defn extract-and-store-quest!
  [quest-title]
  (ensure-item-title-index!)
  (let [quest-details (fetch-quest-details quest-title)

        required-links (wikilinks-in-text (:items quest-details))
        recommended-links (wikilinks-in-text (:recommended quest-details))
        prerequisite-links (wikilinks-in-text (:requirements quest-details))

        required-items (->> required-links (keep canonical-item-title) distinct vec)
        recommended-items (->> recommended-links (keep canonical-item-title) distinct vec)

        prerequisite-quests (->> prerequisite-links
                                 (filter quest-title?)
                                 distinct
                                 vec)]

    (replace-many-attribute! quest-title :quest/required-link required-links)
    (replace-many-attribute! quest-title :quest/recommended-link recommended-links)

    (replace-many-attribute! quest-title :quest/required-item required-items)
    (replace-many-attribute! quest-title :quest/recommended-item recommended-items)

    (replace-many-attribute! quest-title :quest/prerequisite-quest prerequisite-quests)

    {:quest quest-title
     :required-item-count (count required-items)
     :recommended-item-count (count recommended-items)
     :prerequisite-quest-count (count prerequisite-quests)}))

(defn extract-and-store-all-quests!
  ([] (extract-and-store-all-quests! {:print-every 25}))
  ([{:keys [print-every] :or {print-every 25}}]
   (ensure-item-title-index!)
   (let [titles (all-quest-titles)
         total (count titles)]
     (reduce
      (fn [{:keys [index] :as acc} title]
        (let [index' (inc index)
              result (extract-and-store-quest! title)]
          (when (or (= index' 1) (= index' total) (zero? (mod index' print-every)))
            (println (format "[%d/%d] %s req=%d rec=%d prereq=%d"
                             index' total title
                             (:required-item-count result)
                             (:recommended-item-count result)
                             (:prerequisite-quest-count result))))
          {:index index'}))
      {:index 0}
      titles))))
