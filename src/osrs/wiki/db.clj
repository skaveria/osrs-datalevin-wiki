
(ns osrs.wiki.db
  (:require
   [datalevin.core :as datalevin]))

(def database-directory
  "data/wiki-dl")

(def schema
  ;; -------------------------
  ;; Raw mirror layer
  ;; -------------------------
  {:wiki/title           {:db/valueType :db.type/string
                          :db/unique :db.unique/identity}
   :wiki/page-id         {:db/valueType :db.type/long}
   :wiki/ns              {:db/valueType :db.type/long}
   :wiki/revision-id     {:db/valueType :db.type/long}
   :wiki/parent-id       {:db/valueType :db.type/long}
   :wiki/timestamp       {:db/valueType :db.type/string}
   :wiki/wikitext        {:db/valueType :db.type/string}
   :wiki/ingested-at     {:db/valueType :db.type/string}
   :wiki/error           {:db/valueType :db.type/string}

   ;; -------------------------
   ;; Derived layer: infobox tags
   ;; -------------------------
   :wiki/infobox-name       {:db/valueType :db.type/string
                             :db/cardinality :db.cardinality/many}
   :wiki/infobox-params-edn {:db/valueType :db.type/string
                             :db/cardinality :db.cardinality/many}

   ;; -------------------------
   ;; Derived layer: quest graph
   ;; -------------------------
   :quest/prerequisite-quest {:db/valueType :db.type/string
                              :db/cardinality :db.cardinality/many}

   ;; Raw links (debuggable)
   :quest/required-link      {:db/valueType :db.type/string
                              :db/cardinality :db.cardinality/many}
   :quest/recommended-link   {:db/valueType :db.type/string
                              :db/cardinality :db.cardinality/many}

   ;; Canonical item titles
   :quest/required-item      {:db/valueType :db.type/string
                              :db/cardinality :db.cardinality/many}
   :quest/recommended-item   {:db/valueType :db.type/string
                              :db/cardinality :db.cardinality/many}})

(defonce connection-atom (atom nil))

(defn open-database!
  []
  (when-not @connection-atom
    (reset! connection-atom
            (datalevin/get-conn database-directory schema)))
  @connection-atom)

(defn close-database!
  []
  (when-let [conn @connection-atom]
    (datalevin/close conn)
    (reset! connection-atom nil))
  :ok)

(defn database
  []
  (datalevin/db (open-database!)))

(defn transact!
  "Transact a tx vector (maps and/or [:db/add ...] forms)."
  [tx]
  (datalevin/transact! (open-database!) tx))

(defn query
  "Run a datalog query."
  [query-form & inputs]
  (apply datalevin/q query-form (database) inputs))
