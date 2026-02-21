(ns osrs.wiki-extract
  "Local-only extraction: read stored :wiki/wikitext, parse infobox templates,
  transact normalized facts back into the same Datalevin DB.

  No MediaWiki API calls here."
  (:require
   [clojure.string :as str]
   [datalevin.core :as dl]
   [clojure.edn :as edn]
   [clojure.java.io :as io]))

;; -----------------------------------------------------------------------------
;; DB (point this at the same DB dir you used for wiki-ingest)
;; -----------------------------------------------------------------------------

(def db-dir "data/wiki-pages-dl")


(def schema
  "Extend the ingest schema with extracted fields. Safe to keep adding."
  {;; identity / ingest
   :wiki/title       {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :wiki/page-id     {:db/valueType :db.type/long}
   :wiki/ns          {:db/valueType :db.type/long}
   :wiki/revision-id {:db/valueType :db.type/long}
   :wiki/parent-id   {:db/valueType :db.type/long}
   :wiki/timestamp   {:db/valueType :db.type/string}
   :wiki/wikitext    {:db/valueType :db.type/string}
   :wiki/raw-edn     {:db/valueType :db.type/string}
   :wiki/categories  {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}
   :wiki/error       {:db/valueType :db.type/string}

   ;; ------------------------------------------------------------
   ;; extracted: items (typed core fields)
   ;; ------------------------------------------------------------
   :osrs/item-id     {:db/valueType :db.type/long}
   :osrs/name        {:db/valueType :db.type/string}
   :osrs/examine     {:db/valueType :db.type/string}
   :osrs/value       {:db/valueType :db.type/long}
   :osrs/weight      {:db/valueType :db.type/double}
   :osrs/members?    {:db/valueType :db.type/boolean}
   :osrs/tradeable?  {:db/valueType :db.type/boolean}

   ;; NEW: additional high-coverage booleans
   :osrs/stackable?  {:db/valueType :db.type/boolean}
   :osrs/equipable?  {:db/valueType :db.type/boolean}
   :osrs/noteable?   {:db/valueType :db.type/boolean}
   :osrs/placeholder? {:db/valueType :db.type/boolean}
   :osrs/quest?      {:db/valueType :db.type/boolean}

   ;; NEW: common strings
   :osrs/options     {:db/valueType :db.type/string}
   :osrs/update      {:db/valueType :db.type/string}
   :osrs/release     {:db/valueType :db.type/string}
   :osrs/image       {:db/valueType :db.type/string}

   ;; NEW: mid-coverage extras (leave as string/bool)
   :osrs/exchange?   {:db/valueType :db.type/boolean}
   :osrs/alchable?   {:db/valueType :db.type/boolean}
   :osrs/bankable?   {:db/valueType :db.type/boolean}
   :osrs/destroy     {:db/valueType :db.type/string}
   :osrs/leagueRegion {:db/valueType :db.type/string}

   ;; NEW: store *all* whitelisted infobox keys as a single EDN blob (string)
   ;; so we can explore without schema explosion.
   :osrs/item/infobox {:db/valueType :db.type/string}

   ;; ------------------------------------------------------------
   ;; extracted: monsters (minimal)
   ;; ------------------------------------------------------------
   :osrs/monster/combat     {:db/valueType :db.type/long}
   :osrs/monster/hitpoints  {:db/valueType :db.type/long}
   :osrs/monster/slayer-lvl {:db/valueType :db.type/long}
   :osrs/monster/slayer-xp  {:db/valueType :db.type/long}
   :osrs/monster/members?   {:db/valueType :db.type/boolean}
   :osrs/monster/npc-id     {:db/valueType :db.type/long}

   ;; ------------------------------------------------------------
   ;; provenance/debug
   ;; ------------------------------------------------------------
   :osrs/infobox/raw-edn {:db/valueType :db.type/string}})

(defonce conn* (atom nil))

(defn open-db! []
  (when-not @conn*
    (reset! conn* (dl/get-conn db-dir schema)))
  @conn*)

(defn close-db! []
  (when-let [c @conn*]
    (dl/close c)
    (reset! conn* nil))
  :ok)

(defn db [] (dl/db (open-db!)))

(defn q [query & inputs]
  (apply dl/q query (db) inputs))

(defn transact! [tx]
  (dl/transact! (open-db!) tx))

(defn sleep-ms! [ms]
  (when (and ms (pos? ms))
    (Thread/sleep (long ms))))

;; -----------------------------------------------------------------------------
;; Wikitext template parsing (good-enough)
;; -----------------------------------------------------------------------------

(defn extract-template-block
  "Extract the first occurrence of a template block by name, counting {{ }} nesting.
  Returns raw string including braces, or nil."
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
  "Parse lines like: |key = value   into {:key \"value\"}."
  [template-block]
  (->> (str/split-lines (or template-block ""))
       (keep (fn [line]
               (when (str/starts-with? line "|")
                 (let [without (subs line 1)
                       parts (str/split without #"\s*=\s*" 2)
                       k (some-> (first parts) str/trim)
                       v (some-> (second parts) str/trim)]
                   (when (and k v (not (str/blank? k)))
                     [(keyword k) v])))))
       (into {})))

(defn template
  [wikitext template-name]
  (when-let [block (extract-template-block wikitext template-name)]
    (parse-template-params block)))

;; -----------------------------------------------------------------------------
;; Normalization helpers
;; -----------------------------------------------------------------------------

(defn nonblank [s]
  (let [t (when (string? s) (str/trim s))]
    (when (and t (not (str/blank? t))) t)))

(defn parse-bool [s]
  (let [t (some-> s nonblank str/lower-case)]
    (cond
      (nil? t) nil
      (#{"yes" "true" "1"} t) true
      (#{"no" "false" "0"} t) false
      :else nil)))

(defn parse-long [s]
  (let [t (some-> s nonblank (str/replace #"," "") (str/replace #"^\+" ""))]
    (when t
      (try (Long/parseLong t) (catch Exception _ nil)))))

(defn parse-double [s]
  (let [t (some-> s nonblank (str/replace #"," "") (str/replace #"^\+" ""))]
    (when t
      (try (Double/parseDouble t) (catch Exception _ nil)))))

(defn remove-nils [m]
  (into {} (remove (fn [[_ v]] (nil? v)) m)))

;; -----------------------------------------------------------------------------
;; Extraction: Item + Monster
;; -----------------------------------------------------------------------------

(defn extract-item
  "Extract/normalize Infobox Item from wikitext. Returns entity map or nil."
  [title wikitext]
  (when-let [ibox (template wikitext "Infobox Item")]
    (let [id (parse-long (:id ibox))
          ent {:wiki/title title
               :osrs/item-id id
               :osrs/name (nonblank (or (:name ibox) (:Name ibox)))
               :osrs/examine (nonblank (:examine ibox))
               :osrs/value (parse-long (:value ibox))
               :osrs/weight (parse-double (:weight ibox))
               :osrs/members? (parse-bool (:members ibox))
               :osrs/tradeable? (parse-bool (:tradeable ibox))
               :osrs/infobox/raw-edn (pr-str ibox)}]
      (remove-nils ent))))

(defn extract-monster
  "Extract/normalize Infobox Monster from wikitext. Returns entity map or nil."
  [title wikitext]
  (when-let [ibox (template wikitext "Infobox Monster")]
    (let [ent {:wiki/title title
               :osrs/monster/combat (parse-long (:combat ibox))
               :osrs/monster/hitpoints (parse-long (:hitpoints ibox))
               :osrs/monster/slayer-lvl (parse-long (:slaylvl ibox))
               :osrs/monster/slayer-xp (parse-long (:slayxp ibox))
               :osrs/monster/members? (or (parse-bool (:members ibox))
                                          (parse-bool (:is_members_only ibox)))
               :osrs/monster/npc-id (parse-long (:id ibox))
               :osrs/infobox/raw-edn (pr-str ibox)}]
      (remove-nils ent))))

;; -----------------------------------------------------------------------------
;; Drivers
;; -----------------------------------------------------------------------------

(defn titles-in-category
  "DB-only: return titles that have a given category tag."
  [category-title]
  (map first
       (q '[:find ?t
            :in $ ?cat
            :where
            [?e :wiki/title ?t]
            [?e :wiki/categories ?cat]]
          category-title)))

(defn extract-category!
  "Extract either :item or :monster for every page in a category that has wikitext.
  kind = :item | :monster"
  [{:keys [category kind sleep-ms] :or {sleep-ms 0}}]
  (open-db!)
  (let [titles (vec (distinct (titles-in-category category)))
        total  (count titles)]
    (reduce
     (fn [{:keys [i stored skipped] :as acc} title]
       (sleep-ms! sleep-ms)
       (let [wt (ffirst
                 (q '[:find ?wt
                      :in $ ?t
                      :where
                      [?e :wiki/title ?t]
                      [?e :wiki/wikitext ?wt]]
                    title))
             ent (when (string? wt)
                   (case kind
                     :item   (extract-item title wt)
                     :monster (extract-monster title wt)
                     nil))]
         (cond
           (nil? wt)
           (do
             (when (zero? (mod (inc i) 500))
               (println (format "[%d/%d] %s (no wikitext)" (inc i) total title)))
             (assoc acc :i (inc i) :skipped (inc skipped)))

           (nil? ent)
           (do
             (when (zero? (mod (inc i) 500))
               (println (format "[%d/%d] %s (no %s infobox)" (inc i) total title (name kind))))
             (assoc acc :i (inc i) :skipped (inc skipped)))

           :else
           (do
             (transact! [ent])
             (let [stored' (inc stored)
                   i' (inc i)]
               (when (zero? (mod stored' 500))
                 (println (format "[%d/%d] stored=%d latest=%s"
                                  i' total stored' title)))
               {:i i' :stored stored' :skipped skipped})))))
     {:i 0 :stored 0 :skipped 0}
     titles)))
