(ns user
  "REPL toolbelt for exploring the OSRS Wiki via the MediaWiki API,
  gently massaging wikitext into maps, and (optionally) storing/querying
  normalized item facts in DataLevin.

  Intended usage:
    ;; Wiki poking:
    (page-head \"Dragon longsword\")
    (peek-wikitext \"Dragon longsword\" 200)
    (item \"Dragon longsword\")
    (pick (-> (item \"Dragon longsword\") :item)
          [:osrs/item-id :osrs/examine :osrs/value :osrs/weight])

    ;; DataLevin playground:
    (open-database!)
    (store-item! \"Dragon longsword\")
    (query-database '[:find ?name ?value
                      :where
                      [?e :osrs/name ?name]
                      [?e :osrs/value ?value]])"
  (:require
   [clj-http.client :as http]
   [cheshire.core :as json]
   [datalevin.core :as datalevin]
   [clojure.string :as str]
   [clojure.pprint :as pp]))

;; -----------------------------------------------------------------------------
;; Configuration
;; -----------------------------------------------------------------------------

(def wiki-api-url
  "The OSRS Wiki MediaWiki API endpoint."
  "https://oldschool.runescape.wiki/api.php")

(def ^:dynamic *user-agent*
  "Be polite. A descriptive User-Agent helps site operators if you ever cause load."
  "osrs-datalevin-repl/0.1 (personal project)")

(def ^:dynamic *sleep-ms*
  "If you call the API in a loop, sleeping a bit keeps your requests gentle."
  150)

(defn- sleep!
  "Tiny helper to avoid repeating Thread/sleep everywhere."
  [ms]
  (when (and ms (pos? ms))
    (Thread/sleep (long ms))))

(defn sleep-ms!
  "Public sleep helper for other namespaces.
  (We keep the private sleep! for internal use if you want.)"
  [ms]
  (sleep! ms))

(defn show
  "Pretty-print any value and return it (nice REPL ergonomics)."
  [x]
  (pp/pprint x)
  x)

(defn keys-of
  "Return sorted keys of a map (for REPL discovery)."
  [m]
  (sort (keys m)))

(defn pick
  "Pick keys from a map."
  [m ks]
  (select-keys m ks))

;; -----------------------------------------------------------------------------
;; Low-level MediaWiki API access
;; -----------------------------------------------------------------------------

(defn wiki-query
  "Call the MediaWiki API with query parameters and return a Clojure map.

  - Always requests JSON
  - Keywordizes JSON keys
  - If the server returns HTML (rare but happens), returns a diagnostic map
    instead of throwing a JSON parse exception."
  [params]
  (let [request-opts {:query-params (merge {:format "json"
                                           ;; v2 changes shapes (e.g. :pages becomes a vector),
                                           ;; which we handle below.
                                           :formatversion "2"}
                                          params)
                      :headers {"User-Agent" *user-agent*
                                "Accept" "application/json"}
                      :as :text
                      :throw-exceptions false}
        response     (http/get wiki-api-url request-opts)
        status       (:status response)
        content-type (get-in response [:headers "content-type"])
        body         (or (:body response) "")
        body-trim    (str/trim body)]
    (cond
      ;; success + looks like JSON
      (and (= 200 status)
           (or (str/starts-with? body-trim "{")
               (str/starts-with? body-trim "[")))
      (json/parse-string body true)

      ;; anything else: return something debuggable in-REPL
      :else
      {:wiki/error :non-json-response
       :http/status status
       :http/content-type content-type
       :params params
       :body/snippet (subs body-trim 0 (min 400 (count body-trim)))})))

;; -----------------------------------------------------------------------------
;; Fetch a page + latest revision (wikitext)
;; -----------------------------------------------------------------------------

(defn page-raw
  "Fetch raw MediaWiki response for a title, including latest revision content."
  [title]
  (wiki-query {:action "query"
               :titles title
               :prop "revisions"
               :rvslots "main"
               :rvprop "ids|timestamp|content"}))

(defn- first-page
  "MediaWiki can return [:query :pages] as either:
    - a vector (formatversion=2)
    - a map keyed by pageid (older formats)
  This returns the first page in either case."
  [response]
  (let [pages (get-in response [:query :pages])]
    (cond
      (vector? pages) (first pages)
      (map? pages)    (first (vals pages))
      :else           nil)))

(defn page-head
  "Return a compact map for a title. Includes wikitext when available.
  If wiki-query returns {:wiki/error ...}, it is passed through unchanged."
  [title]
  (let [response (page-raw title)]
    (if (:wiki/error response)
      response
      (let [page     (first-page response)
            revision (first (:revisions page))
            ;; formatversion=2 commonly uses :content.
            ;; legacy uses :*.
            wikitext (or (get-in revision [:slots :main :content])
                         (get-in revision [:slots :main :*])
                         (:* revision))]
        {:wiki/page-id     (:pageid page)
         :wiki/title       (:title page)
         :wiki/ns          (:ns page)
         :wiki/revision-id (:revid revision)
         :wiki/parent-id   (:parentid revision)
         :wiki/timestamp   (:timestamp revision)
         :wiki/wikitext    wikitext}))))

(defn wikitext
  "Convenience: fetch just the wikitext string for a title."
  [title]
  (let [m (page-head title)]
    (when-not (:wiki/error m)
      (:wiki/wikitext m))))

(defn pages
  "Fetch multiple pages sequentially (polite)."
  [titles]
  (mapv
   (fn [title]
     (sleep! *sleep-ms*)
     (page-head title))
   titles))

;; -----------------------------------------------------------------------------
;; Template extraction from wikitext
;; -----------------------------------------------------------------------------

(defn extract-template-block
  "Extract the first occurrence of a template block by name.

  Returns the raw template string (including braces), or nil.

  Nil-safe: if wikitext is nil/blank, returns nil."
  [wikitext template-name]
  (when (and (string? wikitext) (not (str/blank? wikitext)))
    (let [needle (str "{{" template-name)
          start-index (str/index-of wikitext needle)]
      (when start-index
        (loop [i start-index
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
                (subs wikitext start-index new-i)
                (recur new-i new-depth)))

            :else
            (recur (inc i) depth)))))))

(defn parse-template-params
  "Parse MediaWiki template parameter lines of the form:

    |key = value

  Returns a map like {:key \"value\" ...}.

  This deliberately ignores:
    - unnamed positional params
    - multiline values that don’t live on a single '|...=...' line"
  [template-block]
  (->> (str/split-lines template-block)
       (keep (fn [line]
               (when (str/starts-with? line "|")
                 (let [without-pipe (subs line 1)
                       parts        (str/split without-pipe #"\s*=\s*" 2)
                       k            (first parts)
                       v            (second parts)]
                   (when (and k v)
                     [(keyword (str/trim k))
                      (str/trim v)])))))
       (into {})))

(defn template
  "Extract + parse a named template from wikitext. Returns {:k \"v\" ...} or nil."
  [wikitext template-name]
  (let [block (extract-template-block wikitext template-name)]
    (when block
      (parse-template-params block))))

(defn infobox-item
  "Extract+parse {{Infobox Item ...}} from wikitext."
  [wikitext]
  (template wikitext "Infobox Item"))

(defn infobox-bonuses
  "Extract+parse {{Infobox Bonuses ...}} from wikitext."
  [wikitext]
  (template wikitext "Infobox Bonuses"))

(defn infobox
  "Extract+parse a template by name from a page title.
  Example: (infobox \"Dragon longsword\" \"Infobox Item\")"
  [title template-name]
  (let [t (wikitext title)]
    (when t
      (template t template-name))))

(defn item*
  "Raw Infobox Item params (strings)."
  [title]
  (infobox title "Infobox Item"))

;; -----------------------------------------------------------------------------
;; Normalization helpers (string -> useful types)
;; -----------------------------------------------------------------------------

(defn nonblank
  "Trim a string; return nil if blank."
  [s]
  (let [t (when (string? s) (str/trim s))]
    (when (and t (not (str/blank? t))) t)))

(defn parse-bool
  "Parse common wiki booleans: Yes/No/True/False/1/0.
  Returns true/false/nil."
  [s]
  (let [t (some-> s nonblank str/lower-case)]
    (cond
      (nil? t) nil
      (#{"yes" "true" "1"} t) true
      (#{"no" "false" "0"} t) false
      :else nil)))

(defn parse-int
  "Parse an integer from a wiki-ish string.
  Handles commas and leading +."
  [s]
  (let [t (some-> s nonblank (str/replace #"," "") (str/replace #"^\+" ""))]
    (when t
      (try
        (Long/parseLong t)
        (catch Exception _ nil)))))

(defn parse-float
  "Parse a floating number from a wiki-ish string.
  Handles commas and leading +."
  [s]
  (let [t (some-> s nonblank (str/replace #"," "") (str/replace #"^\+" ""))]
    (when t
      (try
        (Double/parseDouble t)
        (catch Exception _ nil)))))

(defn strip-wikilinks
  "Very small convenience:
    [[Foo|bar]] -> bar
    [[Foo]]     -> Foo"
  [s]
  (when (string? s)
    (-> s
        (str/replace #"\[\[[^\|\]]+\|([^\]]+)\]\]" "$1")
        (str/replace #"\[\[([^\]]+)\]\]" "$1"))))

(defn normalize-item-infobox
  "Take an Infobox Item param map and return a normalized map.
  Keeps :osrs/raw so you can always go back to the original strings."
  [infobox-map]
  (let [name-str        (or (:name infobox-map) (:Name infobox-map))
        id-str          (:id infobox-map)
        examine-str     (:examine infobox-map)
        value-str       (:value infobox-map)
        weight-str      (:weight infobox-map)
        members-str     (:members infobox-map)
        tradeable-str   (:tradeable infobox-map)
        stackable-str   (:stackable infobox-map)
        equipable-str   (:equipable infobox-map)
        noteable-str    (:noteable infobox-map)
        placeholder-str (:placeholder infobox-map)
        exchange-str    (:exchange infobox-map)
        options-str     (:options infobox-map)]
    {:osrs/item-id      (parse-int id-str)
     :osrs/name         (some-> name-str strip-wikilinks nonblank)
     :osrs/examine      (some-> examine-str strip-wikilinks nonblank)
     :osrs/value        (parse-int value-str)
     :osrs/weight       (parse-float weight-str)
     :osrs/members?     (parse-bool members-str)
     :osrs/tradeable?   (parse-bool tradeable-str)
     :osrs/stackable?   (parse-bool stackable-str)
     :osrs/equipable?   (parse-bool equipable-str)
     :osrs/noteable?    (parse-bool noteable-str)
     :osrs/placeholder? (parse-bool placeholder-str)
     :osrs/exchange?    (parse-bool exchange-str)
     :osrs/options      (some-> options-str strip-wikilinks nonblank)
     :osrs/raw          infobox-map}))

;; -----------------------------------------------------------------------------
;; REPL-facing one-liners (pleasant entry points)
;; -----------------------------------------------------------------------------

(defn item
  "Fetch a title, extract Infobox Item, return both raw + normalized.

  If the fetch fails or the page has no wikitext, returns a readable error map."
  [title]
  (let [page-info (page-head title)]
    (cond
      (:wiki/error page-info)
      page-info

      (not (string? (:wiki/wikitext page-info)))
      {:wiki/error :missing-wikitext
       :wiki/title title
       :page (dissoc page-info :wiki/wikitext)}

      :else
      (let [wt         (:wiki/wikitext page-info)
            infobox    (infobox-item wt)
            normalized (when infobox (normalize-item-infobox infobox))]
        {:page     (dissoc page-info :wiki/wikitext)
         :infobox  infobox
         :item     normalized}))))

(defn bonuses
  "Fetch a title, extract Infobox Bonuses (raw string values)."
  [title]
  (let [page-info (page-head title)]
    (if (:wiki/error page-info)
      page-info
      (let [wt (:wiki/wikitext page-info)
            b  (infobox-bonuses wt)]
        {:page    (dissoc page-info :wiki/wikitext)
         :bonuses b}))))

(defn peek-wikitext
  "Print the first N characters of a page's wikitext."
  ([title] (peek-wikitext title 600))
  ([title n]
   (let [t (wikitext title)]
     (if (nil? t)
       {:wiki/error :no-wikitext}
       (let [n (min n (count t))]
         (println (subs t 0 n))
         :ok)))))

;; -----------------------------------------------------------------------------
;; DataLevin: clear names, query playground vibe
;; -----------------------------------------------------------------------------

(def database-directory
  "Local folder for the DataLevin database. Add 'data/' to .gitignore."
  "data/osrs-dl")

(def database-schema
  "Minimal schema for item query playground."
  {:osrs/item-id    {:db/valueType :db.type/long :db/unique :db.unique/identity}
   :osrs/name       {:db/valueType :db.type/string}
   :osrs/examine    {:db/valueType :db.type/string}
   :osrs/value      {:db/valueType :db.type/long}
   :osrs/weight     {:db/valueType :db.type/double}
   :osrs/members?   {:db/valueType :db.type/boolean}
   :osrs/tradeable? {:db/valueType :db.type/boolean}

   ;; provenance (handy for debugging / refresh later)
   :wiki/title      {:db/valueType :db.type/string}
   :wiki/page-id    {:db/valueType :db.type/long}
   :wiki/timestamp  {:db/valueType :db.type/string}})

(defonce database-connection
  (atom nil))

(defn open-database!
  "Open the DataLevin database (creates if missing) and apply schema."
  []
  (when-not @database-connection
    (reset! database-connection
            (datalevin/get-conn database-directory database-schema)))
  @database-connection)

(defn close-database!
  "Close the DataLevin database connection."
  []
  (when-let [conn @database-connection]
    (datalevin/close conn)
    (reset! database-connection nil))
  :closed)

(defn database
  "Return the current immutable database value."
  []
  (datalevin/db (open-database!)))

(defn transact!
  "Transact entity maps into the database.
  Named 'transact!' because that's DataLevin's term, but it's the only
  short-ish name left here."
  [entity-maps]
  (datalevin/transact! (open-database!) entity-maps))

(defn query-database
  "Run a Datalog query against the database."
  [query & inputs]
  (apply datalevin/q query (database) inputs))


(defn remove-nil-values
  "Return a map with any nil-valued entries removed."
  [m]
  (into {}
        (remove (fn [[_ v]] (nil? v)) m)))
(defn store-item!
  "Fetch an item from the wiki via (item title), then upsert into DataLevin by
  :osrs/item-id.

  If a page doesn't yield an :osrs/item-id (or other required fields), we skip it
  with a readable result instead of crashing the whole batch."
  [title]
  (let [{:keys [page item] :as result} (item title)]
    (cond
      (:wiki/error result)
      {:stored/items 0
       :wiki/title title
       :error (:wiki/error result)
       :details result}

      (nil? item)
      {:stored/items 0
       :wiki/title title
       :error :no-normalized-item}

      (nil? (:osrs/item-id item))
      {:stored/items 0
       :wiki/title title
       :error :missing-item-id
       :page page
       :item (pick item [:osrs/name :osrs/examine :osrs/value :osrs/weight :osrs/options])}

      :else
      (let [entity
            (remove-nil-values
             (merge
              {:osrs/item-id (:osrs/item-id item)}
              (select-keys item [:osrs/name :osrs/examine :osrs/value :osrs/weight
                                 :osrs/members? :osrs/tradeable? :osrs/options])
              {:wiki/title (:wiki/title page)
               :wiki/page-id (:wiki/page-id page)
               :wiki/timestamp (:wiki/timestamp page)}))]
        (transact! [entity])
        {:stored/items 1
         :osrs/item-id (:osrs/item-id item)
         :wiki/title (:wiki/title page)}))))


(defn store-items!
  "Store many items, one by one (polite).
  Never stops on a single bad page; returns receipts for everything."
  [titles]
  (mapv
   (fn [title]
     (sleep! *sleep-ms*)
     (try
       (store-item! title)
       (catch Exception e
         {:stored/items 0
          :wiki/title title
          :error :exception
          :message (.getMessage e)})))
   titles))
