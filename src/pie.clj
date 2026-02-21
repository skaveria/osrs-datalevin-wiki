(ns osrs.pie
  (:require
   [clojure.string :as str]
   [user :refer [drop-sources-links
                 parse-wikitext
                 infobox
                 transact!
                 query-database
                 sleep! *sleep-ms*]]))

;; ------------------------------------------------------------
;; Filtering helpers
;; ------------------------------------------------------------

(defn pie-source-title?
  [t]
  (and (string? t)
       (not (str/starts-with? t "Raging Echoes League/"))
       (not (#{"Raging Echoes League"
               "Combat level"
               "Hunter"
               "Reward"} t))))

(defn ingredientish-title?
  [t]
  (and (string? t)
       (not (str/blank? t))
       (not (str/starts-with? t "File:"))
       (not (str/starts-with? t "Category:"))
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
;; Pie Drop Analysis
;; ------------------------------------------------------------

(defn pie-droppers
  [pie-name]
  (->> (drop-sources-links pie-name)
       (filter pie-source-title?)
       distinct
       vec))

(defn highest-combat
  [titles]
  (apply max-key
         second
         (query-database
          '[:find ?title ?combat
            :in $ [?t ...]
            :where
            [?e :wiki/title ?title]
            [?e :osrs/monster/combat ?combat]
            [(contains? ?t ?title)]]
          (set titles))))
