(ns osrs.wiki.mirror
  (:require
   [clj-http.client :as http]
   [cheshire.core :as json]
   [clojure.string :as str]
   [osrs.wiki.db :as database])
  (:import
   (java.time Instant)))

(def wiki-api-url
  "https://oldschool.runescape.wiki/api.php")

(def ^:dynamic *user-agent*
  "osrs-wiki-datalevin/0.1 (personal project)")

(def ^:dynamic *sleep-ms*
  150)

(defn sleep-milliseconds!
  [ms]
  (when (and ms (pos? ms))
    (Thread/sleep (long ms))))

(defn now-iso8601
  []
  (.toString (Instant/now)))

(defn mediawiki-api-call
  "Low-level MediaWiki API call. Returns parsed JSON map or {:wiki/error ...}."
  [params]
  (let [response (http/get wiki-api-url
                           {:query-params (merge {:format "json"
                                                 :formatversion "2"}
                                                params)
                            :headers {"User-Agent" *user-agent*
                                      "Accept" "application/json"}
                            :as :text
                            :throw-exceptions false})
        status (:status response)
        body (str/trim (or (:body response) ""))]
    (cond
      (and (= 200 status)
           (or (str/starts-with? body "{")
               (str/starts-with? body "[")))
      (json/parse-string body true)

      :else
      {:wiki/error (str "non-json-response status=" status)
       :http/status status
       :body/snippet (subs body 0 (min 300 (count body)))})))

(defn fetch-latest-page-revision
  "Fetch page metadata + latest wikitext for title."
  [title]
  (mediawiki-api-call {:action "query"
                       :titles title
                       :prop "revisions"
                       :rvslots "main"
                       :rvprop "ids|timestamp|content"}))

(defn store-page!
  "Fetch one page from the wiki and store raw mirror facts in DataLevin."
  [title]
  (sleep-milliseconds! *sleep-ms*)
  (let [response (fetch-latest-page-revision title)]
    (if (:wiki/error response)
      (do
        (database/transact!
         [{:wiki/title title
           :wiki/error (pr-str response)
           :wiki/ingested-at (now-iso8601)}])
        {:stored 0 :wiki/title title :error (:wiki/error response)})
      (let [page (first (get-in response [:query :pages]))
            revision (first (:revisions page))
            wikitext (or (get-in revision [:slots :main :content])
                         (get-in revision [:slots :main :*]))]
        (database/transact!
         [{:wiki/title title
           :wiki/page-id (:pageid page)
           :wiki/ns (:ns page)
           :wiki/revision-id (:revid revision)
           :wiki/parent-id (:parentid revision)
           :wiki/timestamp (:timestamp revision)
           :wiki/wikitext wikitext
           :wiki/ingested-at (now-iso8601)}])
        {:stored 1 :wiki/title title :revision-id (:revid revision)}))))

(defn fetch-category-member-titles
  "Return all page titles that are members of a category (pagination)."
  [category-title]
  (loop [continue-token nil
         titles []]
    (let [params (cond-> {:action "query"
                          :list "categorymembers"
                          :cmtitle category-title
                          :cmlimit "max"}
                   continue-token (merge continue-token))
          response (mediawiki-api-call params)
          batch (mapv :title (get-in response [:query :categorymembers]))
          next-token (:continue response)]
      (if next-token
        (recur next-token (into titles batch))
        (into titles batch)))))

(defn store-category!
  "Fetch all members of a category and store raw mirror facts for each page.
   Verbose progress every N pages."
  [{:keys [category-title sleep-ms print-every]
    :or {sleep-ms *sleep-ms*
         print-every 100}}]
  (binding [*sleep-ms* sleep-ms]
    (let [titles (->> (fetch-category-member-titles category-title)
                      distinct
                      vec)
          total (count titles)]
      (reduce
       (fn [{:keys [index stored skipped errors] :as acc} title]
         (let [index' (inc index)
               result (try
                        (store-page! title)
                        (catch Exception e
                          {:stored 0 :wiki/title title :error :exception :message (.getMessage e)}))
               stored' (+ stored (if (= 1 (:stored result)) 1 0))
               skipped' (+ skipped (if (= 1 (:stored result)) 0 1))
               errors' (+ errors (if (= 0 (:stored result)) 1 0))]
           (when (or (= index' 1)
                     (= index' total)
                     (zero? (mod index' print-every)))
             (println (format "[%d/%d] %-40s | stored=%d skipped=%d errors=%d"
                              index' total title stored' skipped' errors')))
           {:index index' :stored stored' :skipped skipped' :errors errors'}))
       {:index 0 :stored 0 :skipped 0 :errors 0}
       titles))))
