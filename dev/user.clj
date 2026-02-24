(ns user
  (:require
   [osrs.wiki.db :as database]
   [osrs.wiki.mirror :as mirror]
   [osrs.wiki.infobox :as infobox]
   [osrs.wiki.extract.quest :as quest-extract]))

(defn open! [] (database/open-database!))
(defn close! [] (database/close-database!))
