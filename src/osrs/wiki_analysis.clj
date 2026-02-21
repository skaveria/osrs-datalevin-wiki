(ns osrs.wiki-analysis
  "DB-only analysis helpers over the local wiki mirror.

  Depends on osrs.wiki-extract for:
    - open-db!, q, transact!
    - template parsing helpers (template, etc.)"
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [osrs.wiki-extract :as wx]))

;; ------------------------------------------------------------
;; Safe parsing helpers
;; ------------------------------------------------------------

(defn safe-read-edn
  [s]
  (try (edn/read-string s)
       (catch Exception _ nil)))

(defn parse-bool [s]
  (when (string? s)
    (case (str/lower-case (str/trim s))
      ("yes" "true" "1") true
      ("no" "false" "0") false
      nil)))

(defn parse-long [s]
  (when (string? s)
    (let [t (-> s str/trim (str/replace #"," "") (str/replace #"^\+" ""))]
      (try (Long/parseLong t)
           (catch Exception _ nil)))))

(defn parse-double [s]
  (when (string? s)
    (let [t (-> s str/trim (str/replace #"," "") (str/replace #"^\+" ""))]
      (try (Double/parseDouble t)
           (catch Exception _ nil)))))

;; ------------------------------------------------------------
;; Infobox inspection (DB-only)
;; ------------------------------------------------------------

(defn top-infobox-keys
  "Return {:sampled N :parsed M :bad K :top [[k n] ...]} for an infobox name."
  ([infobox-name] (top-infobox-keys infobox-name 5000 50))
  ([infobox-name sample-n top-n]
   (let [rows   (wx/q '[:find ?p
                        :in $ ?name
                        :where
                        [?e :wiki/infobox-name ?name]
                        [?e :wiki/infobox-params-edn ?p]]
                      infobox-name)
         sample (take sample-n rows)
         maps   (->> sample
                     (map first)
                     (map safe-read-edn)
                     (remove nil?))
         freqs  (->> maps (mapcat keys) frequencies)]
     {:sampled (count sample)
      :parsed  (count maps)
      :bad     (- (count sample) (count maps))
      :top     (->> freqs
                    (sort-by val >)
                    (take top-n)
                    (mapv (fn [[k v]] [k v])))})))

(defn top-infobox-item-keys
  ([] (top-infobox-keys "Infobox Item"))
  ([sample-n top-n] (top-infobox-keys "Infobox Item" sample-n top-n)))

;; ------------------------------------------------------------
;; Item normalization backfill (DB-only)
;; ------------------------------------------------------------

(defn normalize-item-entity
  "Turn parsed Infobox Item params into a normalized entity map.
  Stores both typed fields and a full infobox blob."
  [title params infobox-keys]
  (wx/remove-nils
   {:wiki/title        title
    :osrs/item/infobox (pr-str (select-keys params infobox-keys))

    :osrs/item-id      (parse-long (:id params))
    :osrs/name         (:name params)
    :osrs/examine      (:examine params)
    :osrs/value        (parse-long (:value params))
    :osrs/weight       (parse-double (:weight params))

    :osrs/members?     (parse-bool (:members params))
    :osrs/tradeable?   (parse-bool (:tradeable params))
    :osrs/stackable?   (parse-bool (:stackable params))
    :osrs/equipable?   (parse-bool (:equipable params))
    :osrs/noteable?    (parse-bool (:noteable params))
    :osrs/placeholder? (parse-bool (:placeholder params))
    :osrs/quest?       (parse-bool (:quest params))
    :osrs/exchange?    (parse-bool (:exchange params))
    :osrs/alchable?    (parse-bool (:alchable params))
    :osrs/bankable?    (parse-bool (:bankable params))

    :osrs/options      (:options params)
    :osrs/update       (:update params)
    :osrs/release      (:release params)
    :osrs/image        (:image params)
    :osrs/destroy      (:destroy params)
    :osrs/leagueRegion (:leagueRegion params)}))

(defn backfill-items!
  "Backfill normalized item datoms from stored Infobox Item params.
  Prints every `print-every` stored entities.

  Defensive against odd/partial rows coming back from datalevin spill vectors."
  ([] (backfill-items! 5000 2000))
  ([sample-n print-every]
   (wx/open-db!)
   (let [infobox-keys (->> (top-infobox-item-keys sample-n 80) :top (map first) vec)
         rows-raw (wx/q '[:find ?t ?p
                          :where
                          [?e :wiki/infobox-name "Infobox Item"]
                          [?e :wiki/title ?t]
                          [?e :wiki/infobox-params-edn ?p]])
         ;; normalize rows into plain vectors and drop anything weird
         rows (->> rows-raw
                   (map (fn [r] (when (and (sequential? r) (= 2 (count r))) (vec r))))
                   (remove nil?)
                   vec)
         total (count rows)]
     (reduce
      (fn [{:keys [i stored skipped bad] :as acc} [t p]]
        (let [params (safe-read-edn p)
              ent    (when (map? params) (normalize-item-entity t params infobox-keys))
              i'     (inc i)]
          (cond
            (nil? params)
            {:i i' :stored stored :skipped skipped :bad (inc bad)}

            (or (nil? ent) (nil? (:osrs/item-id ent)))
            {:i i' :stored stored :skipped (inc skipped) :bad bad}

            :else
            (do
              (wx/transact! [ent])
              (let [stored' (inc stored)]
                (when (zero? (mod stored' print-every))
                  (println (format "[%d/%d] stored=%d latest=%s"
                                   i' total stored' t)))
                {:i i' :stored stored' :skipped skipped :bad bad})))))
      {:i 0 :stored 0 :skipped 0 :bad 0}
      rows))))
