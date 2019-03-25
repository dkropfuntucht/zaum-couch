(ns zaum-couch.core
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [zaum.core :as z]))

(defrecord ZaumCouch [connection]
  z/IZaumDatabase
  (z/perform-create
    [_ {:keys [connection level entity] :as command}]
    (let [{:keys [password port user uri scheme]} connection
          result      (http/put (str scheme "://" uri ":" port "/" entity)
                                {:basic-auth       [user password]
                                 :throw-exceptions false})
          body        (json/read-str (:body result)
                                     :key-fn keyword)
          status      (:status result)
          status-code (cond (contains? #{201 202} status)
                            :ok
                            (= status 412)
                            ;;TODO: maybe we have a "treat warnings as errors"
                            :warning
                            :or
                            :error)]
      {:status  status-code
       ;; - for :data we return the empty table [] in a collection of "created" db(s)
       :data    [[]]
       :message (condp = status-code
                  :ok
                  (str "Database " entity " created.")
                  (:reason body))}))

  (z/perform-read
    [_ {:keys [entity identifier]}]
    (let [{:keys [password port user uri scheme]} connection
          path-cont   (if (some? identifier)
                        (str "/" identifier)
                        "/_all_docs")
          result      (http/get (str scheme "://" uri ":" port "/" entity path-cont)
                                {:basic-auth       [user password]
                                 :throw-exceptions false})
          status      (:status result)
          body        (json/read-str (:body result)
                                     :key-fn keyword)
          more        (if (and (contains? #{200} status)
                               (= "/_all_docs" path-cont))
                        (mapv
                         (fn [i]
                           (->
                            (http/get
                             (str scheme "://" uri ":" port "/" entity "/" (:id i))
                             {:basic-auth       [user password]
                              :throw-exceptions false})
                            :body
                            json/read-str))
                         (:rows body))
                        [])
          status-code (cond (contains? #{200} status)
                            :ok
                            :or
                            :error)]
      {:status  status-code
       :data    more
       :message (condp = status-code
                  :ok
                  "reading value"
                  "error occured")}))

  (z/perform-update
    [_ {:keys [record level entity identity]}]
    (let [{:keys [password port user uri scheme]} connection
          method     (if (some? identity) http/put http/post)
          final-uri  (if (some? identity)
                       (str scheme "://" uri ":" port "/" entity "/" identity)
                       (str scheme "://" uri ":" port "/" entity))
          result     (method final-uri
                             {:body             (json/write-str record)
                              :headers          {:Content-Type "application/json"}
                              :basic-auth       [user password]
                              :throw-exceptions false})
          _ (clojure.pprint/pprint result)
          status      (:status result)
          body        (json/read-str (:body result)
                                     :key-fn keyword)
          status-code (cond (contains? #{201 202} status)
                            :ok
                            :or
                            :error)]
      {:status  status-code
       :data    [[(assoc record :id (:id body))]]
       :message (condp = status-code
                  :ok
                  "stored"
                  "error occured")}))

  (z/perform-delete
    [_ {:keys [connection level entity] :as command}]
    (let [{:keys [password port user uri scheme]} connection
          result     (http/delete (str scheme "://" uri ":" port "/" entity)
                                  {:basic-auth       [user password]
                                   :throw-exceptions false})
          body        (json/read-str (:body result)
                                     :key-fn keyword)
          status      (:status result)
          status-code (cond (contains? #{200} status)
                            :ok
                            (contains? #{404} status)
                            :warning
                            :or
                            :error)]
      {:status  status-code
       ;; - what should we return for a deleted database?
       :data    [[]]
       :message (condp = status-code
                  :ok
                  (str "Database " entity " deleted.")
                  (:reason body))})))

(defmethod z/prepare-connection :couch
  [connection-map]
  (ZaumCouch. connection-map))
