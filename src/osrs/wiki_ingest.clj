(ns osrs.wiki-ingest
  "Minimal MediaWiki API -> DataLevin ingest.
   Simple. Direct. No cross-namespace alias weirdness."
  (:require
   [clj-http.client :as http]
   [cheshire.core :as json]
   [clojure.string :as str]
   [datalevin.core :as dl]))

;; ------------------------------------------------------------
;; Config
;; ------------------------------------------------------------

(def wiki-api-url
  "https://oldschool.runescape.wiki/api.php")

(def ^:dynamic *user-agent*
  "osrs-datalevin-wiki/0.1 (personal sandbox)")

(def ^:dynamic *sleep-ms*
  100)

(defn sleep-ms!
  [ms]
  (when (and ms (pos? ms))
    (Thread/sleep (long ms))))

;; ------------------------------------------------------------
;; DB
;; ------------------------------------------------------------

(def db-dir
  "data/wiki-pages-dl")

(def schema
  {:wiki/title       {:db/valueType :db.type/string
                      :db/unique :db.unique/identity}

   :wiki/page-id     {:db/valueType :db.type/long}
   :wiki/ns          {:db/valueType :db.type/long}

   :wiki/revision-id {:db/valueType :db.type/long}
   :wiki/parent-id   {:db/valueType :db.type/long}
   :wiki/timestamp   {:db/valueType :db.type/string}

   :wiki/wikitext    {:db/valueType :db.type/string}
   :wiki/raw-edn     {:db/valueType :db.type/string}

   :wiki/categories  {:db/valueType :db.type/string
                      :db/cardinality :db.cardinality/many}

   :wiki/error       {:db/valueType :db.type/string}})

(defonce conn* (atom nil))

(defn open-db!
  []
  (when-not @conn*
    (reset! conn* (dl/get-conn db-dir schema)))
  @conn*)

(defn close-db!
  []
  (when-let [c @conn*]
    (dl/close c)
    (reset! conn* nil))
  :ok)

(defn db []
  (dl/db (open-db!)))

(defn q
  [query & inputs]
  (apply dl/q query (db) inputs))

(defn transact!
  [tx]
  (dl/transact! (open-db!) tx))

;; ------------------------------------------------------------
;; HTTP
;; ------------------------------------------------------------

(defn wiki-get
  [params]
  (let [resp (http/get wiki-api-url
                       {:query-params (merge {:format "json"
                                              :formatversion "2"}
                                             params)
                        :headers {"User-Agent" *user-agent*}
                        :as :text
                        :throw-exceptions false})
        status (:status resp)
        body   (str/trim (or (:body resp) ""))]
    (cond
      (not= 200 status)
      {:error :http
       :status status}

      (not (str/starts-with? body "{"))
      {:error :non-json}

      :else
      {:ok (json/parse-string body true)})))

;; ------------------------------------------------------------
;; Fetch page
;; ------------------------------------------------------------

(defn fetch-page
  [title]
  (let [res (wiki-get {:action "query"
                       :redirects "1"
                       :titles title
                       :prop "revisions|categories"
                       :cllimit "max"
                       :rvslots "main"
                       :rvprop "ids|timestamp|content"})]
    (if-let [err (:error res)]
      res
      (let [raw (:ok res)
            page (first (get-in raw [:query :pages]))
            rev  (first (:revisions page))
            wt   (get-in rev [:slots :main :content])
            cats (->> (:categories page)
                      (map :title)
                      vec)]
        {:ok {:wiki/title       (:title page)
              :wiki/page-id     (:pageid page)
              :wiki/ns          (:ns page)
              :wiki/revision-id (:revid rev)
              :wiki/parent-id   (:parentid rev)
              :wiki/timestamp   (:timestamp rev)
              :wiki/wikitext    wt
              :wiki/categories  cats}
         :raw raw}))))

;; ------------------------------------------------------------
;; Store page
;; ------------------------------------------------------------

(defn page-already-stored?
  [title revision-id]
  (boolean
   (ffirst
    (q '[:find ?e
         :in $ ?t ?rid
         :where
         [?e :wiki/title ?t]
         [?e :wiki/revision-id ?rid]]
       title revision-id))))

(defn store-page!
  [title]
  (let [res (fetch-page title)]
    (if-let [err (:error res)]
      (do
        (transact! [{:wiki/title title
                     :wiki/error (name err)}])
        {:stored 0 :error err})
      (let [{:keys [ok raw]} res
            rid (:wiki/revision-id ok)]
        (if (page-already-stored? (:wiki/title ok) rid)
          {:stored 0 :skipped :same-revision}
          (do
            (transact! [(assoc ok :wiki/raw-edn (pr-str raw))])
            {:stored 1
             :wiki/title (:wiki/title ok)
             :revision-id rid}))))))

;; ------------------------------------------------------------
;; Category pagination
;; ------------------------------------------------------------

(defn all-category-member-titles
  [category-title]
  (loop [continue-token nil
         titles []]
    (let [res (wiki-get (merge {:action "query"
                                :list "categorymembers"
                                :cmtitle category-title
                                :cmlimit "max"}
                               continue-token))]
      (if-let [err (:error res)]
        (throw (ex-info "Category fetch failed" res))
        (let [raw (:ok res)
              batch (map :title (get-in raw [:query :categorymembers]))
              next  (:continue raw)]
          (if next
            (recur next (into titles batch))
            (into titles batch)))))))

;; ------------------------------------------------------------
;; Bulk ingest
;; ------------------------------------------------------------

(defn store-all-pages!
  [titles]
  (open-db!)
  (let [total (count titles)]
    (reduce
     (fn [{:keys [i stored skipped errors] :as acc} title]
       (sleep-ms! *sleep-ms*)
       (let [r (store-page! title)
             i' (inc i)
             stored'  (if (= 1 (:stored r)) (inc stored) stored)
             skipped' (if (= :same-revision (:skipped r)) (inc skipped) skipped)
             errors'  (if (:error r) (inc errors) errors)]
         (println
          (format "[%d/%d] %-40s | stored=%d skipped=%d errors=%d"
                  i' total title stored' skipped' errors'))
         {:i i'
          :stored stored'
          :skipped skipped'
          :errors errors'}))
     {:i 0 :stored 0 :skipped 0 :errors 0}
     titles)))
