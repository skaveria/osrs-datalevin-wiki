(ns consumables
  "Utilities for discovering food and drink items from the OSRS Wiki."
  (:require
   [user :as u]
   [clojure.string :as str]))

;; -----------------------------------------------------------------------------
;; Step 1: Get all titles from the Food/All_food page
;; -----------------------------------------------------------------------------

(defn page-links
  "Return parsed links from a wiki page using MediaWiki parse API."
  [page-title]
  (let [response (u/wiki-query {:action "parse"
                                 :page page-title
                                 :prop "links"})]
    (get-in response [:parse :links])))

(defn mainspace-titles
  "Keep only main namespace (ns=0) titles.
   These are usually actual content pages (items, etc)."
  [links]
  (->> links
       (filter #(= 0 (:ns %)))
       (map :title)
       distinct
       vec))

(defn all-food-titles
  "Return all page titles listed on Food/All_food."
  []
  (-> "Food/All_food"
      page-links
      mainspace-titles))

;; -----------------------------------------------------------------------------
;; Step 2: Filter to actual consumable items
;; -----------------------------------------------------------------------------

(defn consumable-title?
  "True if the page has an Infobox Item whose options contain Eat or Drink."
  [title]
  (let [raw-infobox (u/item* title)
        options     (:options raw-infobox)]
    (and raw-infobox
         (string? options)
         (or (str/includes? options "Eat")
             (str/includes? options "Drink")))))

(defn filter-consumables
  [titles]
  (reduce
   (fn [acc title]
     (println "Checking:" title)
     (Thread/sleep (long u/*sleep-ms*))
     (if (consumable-title? title)
       (conj acc title)
       acc))
   []
   titles))

(defn all-consumable-titles
  "Return all consumable item titles discovered from Food/All_food."
  []
  (filter-consumables (all-food-titles)))
