(ns user-pie-repl)
;; src/user_pie_repl.clj
;; REPL-only helpers that extend the existing `user` namespace.
;; Load with: (load-file "src/user_pie_repl.clj")

(in-ns 'user)

(require '[clojure.string :as str])

;; ------------------------------------------------------------
;; Template expansion helpers (from REPL)
;; ------------------------------------------------------------

(defn parse-wikitext
  "Ask MediaWiki to parse arbitrary wikitext and return the :parse map."
  [wikitext]
  (:parse
   (wiki-query {:action "parse"
                :contentmodel "wikitext"
                :text wikitext
                :prop "links"})))

(defn drop-sources-links
  "Return mainspace (ns=0) page titles produced by {{Drop sources|...}}."
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
;; Pie-mode filtering helpers (from REPL)
;; ------------------------------------------------------------

(defn pie-source-title?
  "Filter out obvious non-source noise from Drop sources expansion."
  [t]
  (and (string? t)
       (not (str/starts-with? t "Raging Echoes League/"))
       (not (#{"Raging Echoes League"
               "Combat level"
               "Hunter"
               "Reward"} t))))

(defn ingredientish-title?
  "Filter out obvious junk in the 'Creation' snippet when extracting candidate
  ingredient tokens."
  [t]
  (and (string? t)
       (not (str/blank? t))
       (not (or (str/starts-with? t "File:")
                (str/starts-with? t "Category:")))
       (not (#{"Hitpoints"
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
;; Wikitext snippet + token extraction (from REPL)
;; ------------------------------------------------------------

(defn snippet
  "Return a substring of a page's wikitext around the first occurrence of marker."
  [title marker radius]
  (let [wt (wikitext title)]
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
  "Extract the left-hand target of [[Target|...]] / [[Target]] from wikitext.
  This is intentionally simple (good enough for REPL exploration)."
  [text]
  (when (string? text)
    (->> (re-seq #"\[\[([^\]|#]+)" text)
         (map second)
         (map str/trim)
         distinct
         vec)))

;; ------------------------------------------------------------
;; Meat pie: ingredient extraction (what we did in the REPL)
;; ------------------------------------------------------------

(defn meat-pie-ingredientish
  "Return ingredient-ish tokens for Meat pie, using the ==Creation== section area."
  []
  (let [meatpie-snippet (snippet "Meat pie" "==Item sources==" 1500)
        tokens (distinct (concat (plinks-in-text meatpie-snippet)
                                (wikilinks-in-text meatpie-snippet)))]
    (->> tokens
         (filter ingredientish-title?)
         vec)))
