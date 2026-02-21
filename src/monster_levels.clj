(ns monster-levels
  "Cache + helpers for monster combat levels.

  This namespace:
    - computes combat levels from Infobox Monster
    - caches them on disk as EDN
    - lets you ask (combat-level \"Flambeed\") without re-fetching

  You already have the wiki API + infobox parser in `user`."
  (:require
   [user :as u]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]))

;; -----------------------------------------------------------------------------
;; Where to store the cache
;;
;; If you want this baked into git, put it under resources/.
;; If you want it local-only, put it under data/ and gitignore it.
;; -----------------------------------------------------------------------------

(def combat-cache-path
  "resources/monster-combat-levels.edn")

;; -----------------------------------------------------------------------------
;; IO
;; -----------------------------------------------------------------------------

(defn load-combat-cache
  []
  (let [f (io/file combat-cache-path)]
    (if (.exists f)
      (with-open [r (java.io.PushbackReader. (io/reader f))]
        (edn/read {:eof {}} r))
      {})))

(defn save-combat-cache!
  "Write the cache map to disk as EDN."
  [cache-map]
  (let [f (io/file combat-cache-path)]
    (.mkdirs (.getParentFile f))
    (spit f (pr-str cache-map))
    {:saved true
     :path combat-cache-path
     :count (count cache-map)}))

;; -----------------------------------------------------------------------------
;; Fetching combat levels from the wiki
;; -----------------------------------------------------------------------------

(defn monster-infobox
  "Fetch and parse Infobox Monster for a monster page title."
  [title]
  (u/infobox title "Infobox Monster"))

(defn combat-level-from-infobox
  "Extract combat level from an Infobox Monster map.
  Returns a Long or nil."
  [infobox]
  (when (map? infobox)
    (some-> (:combat infobox)
            u/parse-int)))

(defn fetch-combat-level
  "Fetch combat level for a monster title from the wiki (no cache).
  Returns a Long or nil."
  [title]
  (combat-level-from-infobox (monster-infobox title)))

;; -----------------------------------------------------------------------------
;; Public API: cached combat lookup
;; -----------------------------------------------------------------------------

(defn combat-level
  "Return combat level for a monster title.
  Uses cache if available; otherwise fetches from wiki (and returns nil if unknown)."
  [title]
  (let [cache (load-combat-cache)]
    (or (get cache title)
        (fetch-combat-level title))))

(defn bake-combat-levels!
  "Given monster titles, fetch any missing combat levels and write them to disk.

  Returns a receipt:
    {:requested N :already-known K :fetched F :missing M :saved-count total}

  Notes:
    - This is intentionally sequential and polite.
    - Titles that fail to resolve (no infobox or no :combat) are counted as :missing."
  [titles]
  (let [cache0 (load-combat-cache)
        titles (vec (distinct titles))]
    (loop [i 0
           cache cache0
           already-known 0
           fetched 0
           missing 0]
      (if (>= i (count titles))
        (let [save-result (save-combat-cache! cache)]
          {:requested (count titles)
           :already-known already-known
           :fetched fetched
           :missing missing
           :saved-count (count cache)
           :save save-result})
        (let [title (nth titles i)]
          (if (contains? cache title)
            (recur (inc i) cache (inc already-known) fetched missing)
            (do
              ;; polite pacing (uses your global var)
              (Thread/sleep (long u/*sleep-ms*))
              (let [lvl (fetch-combat-level title)]
                (if (some? lvl)
                  (recur (inc i) (assoc cache title lvl) already-known (inc fetched) missing)
                  (recur (inc i) cache already-known fetched (inc missing)))))))))))

(defn heaviest-by-combat
  "Given titles, return the title with highest combat level using the cache.
  Any titles with unknown combat are ignored."
  [titles]
  (let [rows (->> titles
                  (map (fn [t] {:source t :combat (combat-level t)}))
                  (filter :combat)
                  vec)]
    (when (seq rows)
      (apply max-key :combat rows))))
