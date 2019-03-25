(ns zaum-couch.core-test
  (:require [clojure.test :refer :all]
            [zaum.core :as z]
            [zaum-couch.core :refer :all]))

(deftest test-database-level-operations
  (testing "create a database"
    (let [result (z/perform-op
                  :create
                  {:operation   :create
                   :connection  (z/init-connection {:dbtype   :couch
                                                    :dbname   "couchdb"
                                                    :user     "admin"
                                                    :password "alphaghetti"
                                                    :uri      "127.0.0.1"
                                                    :scheme   "http"
                                                    :port     5984})
                   :level       :db
                   :entity      "test"})]
      (is (= (:status result) :ok))))

  (testing "Warning on recreate a database"
    (let [result (z/perform-op
                  :create
                  {:operation   :create
                   :connection  (z/init-connection {:dbtype   :couch
                                                    :dbname   "couchdb"
                                                    :user     "admin"
                                                    :password "alphaghetti"
                                                    :uri      "127.0.0.1"
                                                    :scheme   "http"
                                                    :port     5984})
                   :level       :db
                   :entity      "test"})]
      (is (= (:status result) :warning))))

  (testing "Delete a database"
    (let [result (z/perform-op
                  :delete
                  {:operation   :delete
                   :connection  (z/init-connection {:dbtype   :couch
                                                    :dbname   "couchdb"
                                                    :user     "admin"
                                                    :password "alphaghetti"
                                                    :uri      "127.0.0.1"
                                                    :scheme   "http"
                                                    :port     5984})
                   :level       :db
                   :entity      "test"})]
      (is (= (:status result) :ok))))

  (testing "Delete a nonexistant database"
    (let [result (z/perform-op
                  :delete
                  {:operation   :delete
                   :connection (z/init-connection {:dbtype   :couch
                                                   :dbname   "couchdb"
                                                   :user     "admin"
                                                   :password "alphaghetti"
                                                   :uri      "127.0.0.1"
                                                   :scheme   "http"
                                                   :port     5984})
                   :level       :db
                   :entity      "test"})]
      (is (= (:status result) :warning)))))

(deftest test-store-a-document
  (testing "Create a Database, Store a Document"
    (let [create-result
          (z/perform-op
           :create
           {:operation   :create
            :connection  (z/init-connection {:dbtype   :couch
                                             :dbname   "couchdb"
                                             :user     "admin"
                                             :password "alphaghetti"
                                             :uri      "127.0.0.1"
                                             :scheme   "http"
                                             :port     5984})
            :level       :db
            :entity      "test"})
          store-result
          (z/perform-op
           :update
           {:operation   :update
            :connection  (z/init-connection {:dbtype   :couch
                                             :dbname   "couchdb"
                                             :user     "admin"
                                             :password "alphaghetti"
                                             :uri      "127.0.0.1"
                                             :scheme   "http"
                                             :port     5984})
            :level       :record
            :entity      "test"
            :record      {:foo "bar"}})
          read-result
          (z/perform-op
           :read
           {:operation   :read
            :connection  (z/init-connection {:dbtype   :couch
                                             :dbname   "couchdb"
                                             :user     "admin"
                                             :password "alphaghetti"
                                             :uri      "127.0.0.1"
                                             :scheme   "http"
                                             :port     5984})
            :level       :record
            :entity      "test"})]
      (println "//////////////////////////////////////////////")
      (clojure.pprint/pprint create-result)
      (println "----------------------------------------------")
      (clojure.pprint/pprint store-result)
      (println "//////////////////////////////////////////////")
      (is (contains? #{:ok :warning} (:status create-result)))
      (is (= (:status store-result) :ok)))))
