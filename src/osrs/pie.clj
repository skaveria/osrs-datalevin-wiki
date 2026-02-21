(ns osrs.pie
  "Pie-locked theorycraft helpers.

  This namespace is intentionally small + explicit:
  - It leans on `user` for the MediaWiki API + Datalevin connection.
  - It provides pie-specific extraction + graph closure helpers."
  (:require
   [clojure.string :as str]
   [user :as u]))

;; ------------------------------------------------------------
;; Filtering helpers
;; ------------------------------------------------------------

(defn pie-source-title?
  "Filter out obvious non-monster noise from Drop sources expansion."
  [t]
  (and (string? t)
       (not (str/starts-with? t "Raging Echoes League/"))
       (not (#{"Raging Echoes League"
               "Combat level"
               "Hunter"
               "Reward"} t))))

(defn ingredientish-title?
  "Filter out obvious junk from 'Creation' snippets when extracting ingredient tokens."
  [t]
  (and (string? t)
       (not (str/blank? t))
       (not (str/starts-with? t "File:"))
       (not (str/starts-with? t "Category:"))
       ;; numbers / years / dates
       (not (re-matches #"\d+" t))
       (not (re-matches #"\d{4}" t))
       (not (re-matches #"\d{1,2}\s+[A-Za-z]+" t))
       (not (#{"Trading Post"
               "Hitpoints"
               "Cooking"
               "Grand Exchange"
               "pie"
               "burn rate"
               "burnt pie"
               "range"
               "cooking range"
               "Cooking range (Lumbridge Castle)"
               "lunar spellbook"
               "Cook's Assistant"
               "Nature Spirit"
               "Bake Pie"
               "Romily Weaklax"
               "Cooks' Guild"} t))))

;; ------------------------------------------------------------
;; MediaWiki template expansion (same trick you used in REPL)
;; ------------------------------------------------------------

(defn parse-wikitext
  "Ask MediaWiki to parse arbitrary wikitext and return the :parse map."
  [wikitext]
  (:parse
   (u/wiki-query {:action "parse"
                  :contentmodel "wikitext"
                  :text wikitext
                  :prop "links"})))

(defn drop-sources-links
  "Return mainspace (ns=0) titles produced by {{Drop sources|...}}."
  [item-title]
  (let [wikitext (str "{{Drop sources|" item-title "}}")
        parsed   (parse-wikitext wikitext)
        links    (:links parsed)]
    (->> links
         (filter #(= 0 (:ns %)))
         (map :title)
         distinct
         vec)))

;; ------------------------------------------------------------
;; DB helpers
;; ------------------------------------------------------------

(defn npc-title?
  "True if this title exists in our monster DB (by :wiki/title)."
  [title]
  (boolean
   (ffirst
    (u/query-database
     '[:find ?e
       :in $ ?t
       :where
       [?e :wiki/title ?t]]
     title))))

(defn combat-of
  "Return combat level for a monster title, or nil."
  [monster-title]
  (ffirst
   (u/query-database
    '[:find ?c
      :in $ ?t
      :where
      [?e :wiki/title ?t]
      [?e :osrs/monster/combat ?c]]
    monster-title)))

(defn only-npcs
  "Filter titles down to ones that exist in the monster DB."
  [titles]
  (->> titles (filter npc-title?) vec))

;; ------------------------------------------------------------
;; Wikitext snippet + token extraction
;; ------------------------------------------------------------

(defn snippet
  "Return a substring of a page's wikitext around the first occurrence of marker."
  [title marker radius]
  (let [wt (u/wikitext title)]
    (when (and (string? wt) (not (str/blank? wt)))
      (when-let [i (str/index-of wt marker)]
        (subs wt
              (max 0 (- i radius))
              (min (count wt) (+ i radius)))))))

(defn plinks-in-text
  "Extract {{plink|...}} targets from a chunk of wikitext."
  [text]
  (when (string? text)
    (->> (re-seq #"\{\{plink\|([^}]+)\}\}" text)
         (map second)
         (map str/trim)
         distinct
         vec)))

(defn wikilinks-in-text
  "Extract [[Target]] / [[Target|...]] left-hand targets from wikitext.
  (Intentionally simple: good enough for REPL-style wiki pages.)"
  [text]
  (when (string? text)
    (->> (re-seq #"\[\[([^\]|#]+)" text)
         (map second)
         (map str/trim)
         distinct
         vec)))

(defn item-ingredients
  "Return ingredient-ish tokens from the ==Creation== area of an item page."
  [item-title]
  (let [s (snippet item-title "==Creation==" 2000)]
    (when (string? s)
      (->> (concat (plinks-in-text s)
                   (wikilinks-in-text s))
           (map str/trim)
           distinct
           (filter ingredientish-title?)
           vec))))

;; ------------------------------------------------------------
;; Implied ingredient edges (needed to make Cow appear for Meat pie)
;; ------------------------------------------------------------

(def implied-ingredient-edges
  {"cooked meat"    ["Raw beef" "Raw bear meat" "Raw rat meat" "Raw rabbit"]
   "cooked chicken" ["Raw chicken"]
   "pastry dough"   ["Pot of flour" "Bucket of water"]
   "pie shell"      ["Pastry dough" "Pie dish"]})

(defn expand-ingredients
  "Add implied ingredient edges for known cooking chain steps."
  [ings]
  (->> ings
       (mapcat (fn [i] (concat [i] (get implied-ingredient-edges i))))
       distinct
       vec))

(defn npc-droppers-of-item
  "NPC droppers for an item title (filtered to actual monster DB titles)."
  [item-title]
  (only-npcs
   (->> (drop-sources-links item-title)
        (filter pie-source-title?)
        vec)))

;; ------------------------------------------------------------
;; Dependency closure: items -> ingredients -> ingredients ... collecting NPCs
;; ------------------------------------------------------------

(defn pie-dependency-closure
  "Return {:items #{...} :monsters #{...}} reachable from root-item.

  depth controls how many ingredient expansions we do. 3–4 is usually plenty."
  [root-item depth]
  (loop [frontier (list root-item)
         seen-items #{}
         monsters #{}
         remaining depth]
    (if (or (empty? frontier) (neg? remaining))
      {:items seen-items
       :monsters monsters}
      (let [item (first frontier)
            rest-frontier (rest frontier)]
        (if (contains? seen-items item)
          (recur rest-frontier seen-items monsters remaining)
          (let [ings0 (or (item-ingredients item) [])
                ings  (expand-ingredients ings0)
                new-items (remove seen-items ings)
                new-monsters (mapcat npc-droppers-of-item ings)]
            (recur (concat rest-frontier new-items)
                   (conj seen-items item)
                   (into monsters new-monsters)
                   (dec remaining))))))))

;; ------------------------------------------------------------
;; Pies: category -> closures -> monsters -> rankings
;; ------------------------------------------------------------

(defn all-category-member-titles
  "Return ALL member titles of a category by following MediaWiki continuation."
  [category-title]
  (loop [continue-token nil
         titles []]
    (let [params (merge {:action "query"
                         :list "categorymembers"
                         :cmtitle category-title
                         :cmlimit "max"}
                        continue-token)
          resp   (u/wiki-query params)
          batch  (map :title (get-in resp [:query :categorymembers]))
          next   (:continue resp)]
      (if next
        (recur next (into titles batch))
        (into titles batch)))))

(defn pie-item-titles
  "Return titles from Category:Pies."
  []
  (->> (all-category-member-titles "Category:Pies")
       distinct
       vec))

(defn pie-closures
  "Compute closure per pie title. Returns {pie-title -> closure-map}."
  [pie-titles depth]
  (into {}
        (for [pie pie-titles]
          (do (u/sleep-ms! u/*sleep-ms*)
              [pie (pie-dependency-closure pie depth)]))))

(defn pie->ingredient-monsters
  "Convert closures to {pie-title -> [monster-title ...]} from the ingredient graph only."
  [closures]
  (into {}
        (for [[pie cl] closures]
          [pie (->> (:monsters cl)
                    distinct
                    sort
                    vec)])))

(defn all-ingredient-monsters
  "Union all ingredient-graph monsters across pies."
  [pie->monsters-map]
  (->> (vals pie->monsters-map)
       (apply concat)
       distinct
       vec))

(defn all-direct-pie-droppers
  "Monsters that drop any pie directly (not via ingredient graph)."
  [pie-titles]
  (->> pie-titles
       (mapcat (fn [pie]
                 (->> (drop-sources-links pie)
                      (filter pie-source-title?)
                      only-npcs)))
       distinct
       vec))

(defn all-pie-monsters-total
  "Union ingredient-graph monsters + direct pie droppers."
  [pie-titles depth]
  (let [closures (pie-closures pie-titles depth)
        pie->mons (pie->ingredient-monsters closures)
        ingredient-mons (all-ingredient-monsters pie->mons)
        direct (all-direct-pie-droppers pie-titles)]
    {:closures closures
     :pie->ingredient-monsters pie->mons
     :ingredient-monsters ingredient-mons
     :direct-pie-droppers direct
     :all-monsters (->> (concat ingredient-mons direct) distinct vec)}))

(defn monsters-with-combat
  "Attach combat levels and sort. Returns [[monster combat] ...]."
  [monster-titles]
  (->> monster-titles
       (map (fn [m] [m (combat-of m)]))
       (remove (fn [[_ c]] (nil? c)))
       (sort-by second >)
       vec))

(defn top-strongest
  "Top N strongest monsters by combat."
  [monsters-with-combat n]
  (take n monsters-with-combat))

(defn top-weakest
  "Top N weakest monsters by combat."
  [monsters-with-combat n]
  (take n (sort-by second < monsters-with-combat)))
