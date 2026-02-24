(ns osrs.wiki.extract.monster
  "Extract monster stats + weaknesses + types + immunities from mirrored wikitext.

  Requires:
    - Pages mirrored into :wiki/wikitext (osrs.wiki.mirror)
    - Infobox tagging already run (osrs.wiki.infobox), so we can enumerate Infobox Monster pages.

  Stores onto entity keyed by :wiki/title:

  Core stats (single-valued):
    :monster/combat-level
    :monster/hitpoints
    :monster/slayer-level
    :monster/slayer-xp
    :monster/npc-id
    :monster/members?

  Types / styles (many-valued):
    :monster/attributes
    :monster/attack-style

  Weakness:
    :monster/weakness-type
    :monster/weakness-percent

  Immunities (many-valued tags like \"Poison\", \"Venom\", \"Cannon\"):
    :monster/immunity

  Defence bonuses (single-valued):
    :monster/defence-stab
    :monster/defence-slash
    :monster/defence-crush
    :monster/defence-magic
    :monster/defence-range
    :monster/defence-light
    :monster/defence-standard
    :monster/defence-heavy"
  (:require
   [clojure.string :as str]
   [osrs.wiki.db :as database]))

;; ------------------------------------------------------------
;; Wikitext template parsing
;; ------------------------------------------------------------

(defn extract-template-block
  "Extract the first occurrence of {{TemplateName ...}} including nested templates.
   Returns raw string including braces, or nil."
  [wikitext template-name]
  (when (and (string? wikitext) (not (str/blank? wikitext)))
    (let [needle (str "{{" template-name)
          start  (str/index-of wikitext needle)]
      (when start
        (loop [i start
               depth 0]
          (cond
            (>= i (count wikitext))
            nil

            ;; Found '{{' -> depth + 1
            (and (< (inc i) (count wikitext))
                 (= \{ (.charAt wikitext i))
                 (= \{ (.charAt wikitext (inc i))))
            (recur (+ i 2) (inc depth))

            ;; Found '}}' -> depth - 1
            (and (< (inc i) (count wikitext))
                 (= \} (.charAt wikitext i))
                 (= \} (.charAt wikitext (inc i))))
            (let [new-depth (dec depth)
                  new-i     (+ i 2)]
              (if (zero? new-depth)
                (subs wikitext start new-i)
                (recur new-i new-depth)))

            :else
            (recur (inc i) depth)))))))

(defn parse-template-params
  "Parse |k=v params supporting multiline values (continuation lines)."
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

(defn parse-infobox-monster
  "Return the param map for {{Infobox Monster ...}} on a page, or nil."
  [wikitext]
  (when-let [block (extract-template-block wikitext "Infobox Monster")]
    (parse-template-params block)))

;; ------------------------------------------------------------
;; Normalization helpers
;; ------------------------------------------------------------

(defn nonblank
  [s]
  (let [t (when (string? s) (str/trim s))]
    (when (and t (not (str/blank? t))) t)))

(defn parse-long-safe
  "Parse the first integer found in a string."
  [s]
  (when-let [s (nonblank s)]
    (when-let [[_ digits] (re-find #".*?([0-9]+).*" s)]
      (try
        (Long/parseLong digits)
        (catch Exception _ nil)))))

(defn parse-yes-no
  "Parse common yes/no markers: Y/Yes/No/N/true/false/1/0."
  [s]
  (let [t (some-> s nonblank str/lower-case)]
    (cond
      (nil? t) nil
      (#{"y" "yes" "true" "1"} t) true
      (#{"n" "no" "false" "0"} t) false
      :else nil)))

(defn strip-wikilinks
  "Tiny convenience:
   [[Foo|bar]] -> bar
   [[Foo]]     -> Foo"
  [s]
  (when (string? s)
    (-> s
        (str/replace #"\[\[[^\|\]]+\|([^\]]+)\]\]" "$1")
        (str/replace #"\[\[([^\]]+)\]\]" "$1"))))

(defn normalize-split-list
  "Turn a wiki-ish list field into a vector of tokens.

  Handles:
    - commas
    - &&SPLITPOINT&&
    - wikilinks
    - newlines"
  [s]
  (let [s (some-> s nonblank strip-wikilinks)]
    (if-not s
      []
      (->> (str/split
            (-> s
                (str/replace "&&SPLITPOINT&&" ",")
                (str/replace #"\n" ","))
            #"\s*,\s*")
           (map (comp nonblank str/trim))
           (remove nil?)
           distinct
           vec))))

(defn immunity?
  "True only for clearly-immune values. Avoids false positives like \"Not immune\"."
  [s]
  (let [t (some-> s nonblank str/lower-case str/trim)]
    (boolean
     (and t
          ;; explicitly reject negations
          (not (or (str/includes? t "not immune")
                   (= t "no")
                   (= t "n")))
          ;; accept immune/immune (weak)/immune (anything) + yes/y
          (or (str/starts-with? t "immune")
              (= t "yes")
              (= t "y"))))))

(defn first-present
  "Return the first nonblank value for the provided keys."
  [m ks]
  (some (fn [k] (nonblank (get m k))) ks))

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

(defn monster-titles
  "Return titles tagged as Infobox Monster."
  []
  (->> (database/query
        '[:find ?t
          :where
          [?e :wiki/title ?t]
          [?e :wiki/infobox-name "Infobox Monster"]])
       (map first)
       distinct
       sort
       vec))

(defn replace-many-attribute!
  "Replace all values of a cardinality-many attribute on [:wiki/title title]."
  [title attribute new-values]
  (let [old-values
        (map first
             (database/query
              '[:find ?v
                :in $ ?t ?a
                :where
                [?e :wiki/title ?t]
                [?e ?a ?v]]
              title attribute))

        new-values
        (->> (or new-values [])
             (filter string?)
             (map str/trim)
             (remove str/blank?)
             distinct
             vec)

        retracts (mapv (fn [v] [:db/retract [:wiki/title title] attribute v]) old-values)
        adds     (mapv (fn [v] [:db/add [:wiki/title title] attribute v]) new-values)]
    (database/transact! (into [] (concat retracts adds)))
    {:title title
     :attribute attribute
     :old (count old-values)
     :new (count new-values)}))

;; ------------------------------------------------------------
;; Extraction
;; ------------------------------------------------------------

(defn normalize-monster-facts
  "Turn an Infobox Monster param map into stable facts."
  [infobox-params]
  (let [combat-text (first-present infobox-params [:combat :combat1 :combat_level])
        combat-level (parse-long-safe combat-text)

        hitpoints (parse-long-safe (:hitpoints infobox-params))

        slayer-level (parse-long-safe (:slaylvl infobox-params))
        slayer-xp    (parse-long-safe (:slayxp infobox-params))

        npc-id (parse-long-safe (:id infobox-params))

        members? (parse-yes-no (first-present infobox-params [:members :is_members_only]))

        attributes (normalize-split-list (or (:attributes infobox-params)
                                             (:attribute infobox-params)))

        attack-style-key  (keyword "attack style")
        attack-style1-key (keyword "attack style1")

        attack-styles (normalize-split-list (or (:attack_style infobox-params)
                                                (get infobox-params attack-style-key)
                                                (:attack_style1 infobox-params)
                                                (get infobox-params attack-style1-key)))

        weakness-type (some-> (:elementalweaknesstype infobox-params) strip-wikilinks nonblank)
        weakness-percent (parse-long-safe (:elementalweaknesspercent infobox-params))

        immunities (->> [["Poison" (:immunepoison infobox-params)]
                         ["Venom"  (:immunevenom infobox-params)]
                         ["Cannon" (:immunecannon infobox-params)]
                         ["Thrall" (:immunethrall infobox-params)]
                         ["Burn"   (:immuneburn infobox-params)]]
                        (keep (fn [[label v]] (when (immunity? v) label)))
                        vec)

        defence-stab     (parse-long-safe (:dstab infobox-params))
        defence-slash    (parse-long-safe (:dslash infobox-params))
        defence-crush    (parse-long-safe (:dcrush infobox-params))
        defence-magic    (parse-long-safe (:dmagic infobox-params))
        defence-range    (parse-long-safe (:drange infobox-params))
        defence-light    (parse-long-safe (:dlight infobox-params))
        defence-standard (parse-long-safe (:dstandard infobox-params))
        defence-heavy    (parse-long-safe (:dheavy infobox-params))]
    {:monster/combat-level combat-level
     :monster/hitpoints hitpoints
     :monster/slayer-level slayer-level
     :monster/slayer-xp slayer-xp
     :monster/npc-id npc-id
     :monster/members? members?

     :monster/attributes attributes
     :monster/attack-style attack-styles

     :monster/weakness-type weakness-type
     :monster/weakness-percent weakness-percent

     :monster/immunity immunities

     :monster/defence-stab defence-stab
     :monster/defence-slash defence-slash
     :monster/defence-crush defence-crush
     :monster/defence-magic defence-magic
     :monster/defence-range defence-range
     :monster/defence-light defence-light
     :monster/defence-standard defence-standard
     :monster/defence-heavy defence-heavy}))


(defn remove-nil-values
  "Return a map with all nil-valued entries removed."
  [m]
  (into {}
        (filter (fn [[_ v]] (some? v)))
        m))
(defn store-monster-facts!
  "Extract monster facts for one page and store them."
  [title]
  (let [wikitext (fetch-page-wikitext title)
        infobox  (when (string? wikitext) (parse-infobox-monster wikitext))]
    (if (not (map? infobox))
      {:stored 0 :wiki/title title :error :no-infobox-monster}
      (let [facts         (normalize-monster-facts infobox)
            attributes    (:monster/attributes facts)
            attack-styles (:monster/attack-style facts)
            immunities    (:monster/immunity facts)

            scalar-facts
            (-> facts
                (dissoc :monster/attributes :monster/attack-style :monster/immunity)
                remove-nil-values)]

        ;; store scalar facts
        (database/transact!
         [(merge {:wiki/title title} scalar-facts)])

        ;; store many-valued facts
        (replace-many-attribute! title :monster/attributes attributes)
        (replace-many-attribute! title :monster/attack-style attack-styles)
        (replace-many-attribute! title :monster/immunity immunities)

        {:stored 1
         :wiki/title title
         :monster/combat-level (:monster/combat-level scalar-facts)
         :monster/hitpoints (:monster/hitpoints scalar-facts)}))))


(defn store-all-monsters!
  "Extract and store monster facts for all Infobox Monster pages.
   Verbose progress every N."
  ([] (store-all-monsters! {:print-every 100}))
  ([{:keys [print-every] :or {print-every 100}}]
   (let [titles (monster-titles)
         total (count titles)]
     (reduce
      (fn [{:keys [index stored skipped] :as acc} title]
        (let [index' (inc index)
              result (store-monster-facts! title)
              stored' (+ stored (:stored result))
              skipped' (+ skipped (if (= 1 (:stored result)) 0 1))]
          (when (or (= index' 1) (= index' total) (zero? (mod index' print-every)))
            (println
             (format "[%d/%d] %-35s | stored=%d skipped=%d"
                     index' total title stored' skipped')))
          {:index index' :stored stored' :skipped skipped'}))
      {:index 0 :stored 0 :skipped 0}
      titles))))
