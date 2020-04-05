(ns com.nomistech.nomis-redis-carmine-demo.code-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [com.nomistech.nomis-redis-carmine-demo.code :as sut]
            [taoensso.carmine :as car]
            [clojure.string :as str]))

;;;; ___________________________________________________________________________

;;;; Some of this is adapted from https://github.com/ptaoussanis/carmine.

;;;; NB: This all assumes that we have Redis running on redis://localhost:6379.

;;;; ___________________________________________________________________________
;;;; Connection

(def my-conn-spec
  {:pool {}
   :spec {:uri "redis://localhost:6379"}})

(defmacro wcar* [& body]
  `(car/wcar my-conn-spec ~@body))

;;;; ___________________________________________________________________________
;;;; `with-del-on-start-and-finish`

(defn with-del-on-start-and-finish*
  [{ks :keys
    no-error-if-not-exists? :no-error-if-not-exists?
    :as opts} fun]
  (when-not (contains? opts :keys)
    (throw (Exception. ":keys key missing from opts")))
  (when-not (sequential? ks)
    (throw (Exception. (str/join " "
                                 ["Value for :keys key is not a collection: got "
                                  (pr-str ks)]))))
  (let [delete-keys #(when (not (empty? ks))
                       (wcar* (apply car/del ks)))]
    (delete-keys)
    (let [result (fun)]
      (when-not no-error-if-not-exists?
        (doseq [k ks]
          (when (not= 1 (wcar* (car/exists k)))
            (throw (Exception.
                    (str/join " "
                              ["with-del-on-start-and-finish:"
                               "The following key does not exist at finish:"
                               (pr-str k)]))))))
      (delete-keys)
      result)))

(defmacro with-del-on-start-and-finish [opts & body]
  `(with-del-on-start-and-finish* ~opts (fn [] ~@body)))

(deftest with-del-on-start-and-finish-test
  (let [[k1 k2 :as ks] ["nomis/demo-key-a"
                        "nomis/demo-key-b"]
        [v1 v2]        ["my-val-1" "my-val-2"]
        set-keys       #(wcar* (car/set k1 v1)
                               (car/set k2 v2))
        count-keys     #(wcar* (car/exists k1 k2))]
    (wcar*
     (set-keys)
     (testing "Before start, keys exist" (is (= 2 (count-keys))))
     (with-del-on-start-and-finish {:keys ks}
       (testing "Just after start, keys don't exist" (is (= 0 (count-keys))))
       (set-keys)
       (testing "After setting them, keys exist" (is (= 2 (count-keys))))))
    (testing "After finish, keys don't exist" (is (= 0 (count-keys))))))

;;;; ___________________________________________________________________________
;;;; Basics

(deftest ping-test
  (is (= "PONG"
         (wcar* (car/ping)))))

(deftest set-and-get-test
  (let [k "nomis/demo-key"
        v "my-val"]
    (wcar*
     (with-del-on-start-and-finish {:keys [k]}
       (is (= "OK" (wcar* (car/set k v))))
       (is (= v    (wcar* (car/get k))))))))

;;;; ___________________________________________________________________________
;;;; Pipelining -- multiple calls to Redis

(deftest for-multiple-redis-calls-we-get-a-vector-of-results
  (let [k "nomis/demo-key"
        v "my-val"]
    (wcar*
     (with-del-on-start-and-finish {:keys [k]}
       (testing "If we execute multiple Redis commands, we get a vector of results"
         (is (= ["PONG" "OK" v]
                (wcar* (car/ping)
                       (car/set k v)
                       (car/get k)))))))))

(deftest more-details-about-vectors-or-not-as-the-result

  ;; This is about what the use, or not, of Redis pipelines (for which,
  ;; see https://redis.io/topics/pipelining).
  ;;
  ;; When we use a pipeline, we get a vector of results.

  (let [ping-n-times (fn [n] (dotimes [_ n] (car/ping)))]

    (testing "Without `:as-pipeline`"

      (testing "A single command returns a single result, not in a vector"
        (is (= "PONG"
               (wcar* (ping-n-times 1)))))

      (testing "Multiple commands return a vector of results"
        ;; TODO It isn't clear to me what exactly is going on here.
        ;;      At what point does Carmine decide we are pipelining?
        ;;      Is everything always executed in a pipeline? Or does Carmine
        ;;      start a pipeline for the second and subsequent commands?
        (is (= ["PONG" "PONG" "PONG" "PONG"]
               (wcar* (ping-n-times 4))))))

    (testing "With `:as-pipeline`"

      (testing "A single command returns a singleton vector result"
        (is (= ["PONG"]
               (wcar* :as-pipeline (ping-n-times 1)))))

      (testing "Multiple commands return a vector of results"
        (is (= ["PONG" "PONG" "PONG" "PONG"]
               (wcar* :as-pipeline (ping-n-times 4))))))))

(deftest be-careful--we-can-have-vectors-as-values
  (testing "We can have a vector value -- so testing for vectors is not a general solution to seeing whether pipelining happened"
    (let [k "nomis/demo-key"
          v [1 2 3]]
      (wcar* (with-del-on-start-and-finish {:keys [k]}
               (wcar* (car/set k v))
               (is (= v (wcar* (car/get k)))))))))

;;;; ___________________________________________________________________________
;;;; Redis errors and JVM exceptions

(deftest throws-exception-on-redis-error-unless-pipelining
  (let [k           "nomis/demo-key-not-a-set"
        v-not-a-set "not-a-set"]

    (testing "Exception when not pipelining"
      (wcar*
       (with-del-on-start-and-finish {:keys [k]}
         (car/set k v-not-a-set)
         (is (thrown-with-msg?
              Exception
              #"WRONGTYPE Operation against a key holding the wrong kind of value"
              (wcar* (car/spop k)))))))

    (testing "No exception when pipelining"
      (wcar*
       (with-del-on-start-and-finish {:keys [k]}
         (car/set k v-not-a-set)
         (let [[r1 r2 & rs] (wcar*
                             (car/exists k)
                             (car/spop k))]
           (is (= 1 r1))
           (is (= clojure.lang.ExceptionInfo
                  (type r2)))
           (is (= {:prefix :wrongtype}
                  (ex-data r2)))
           (is (nil? rs))))))

    (testing "No exception when pipelining, even when only one call to Redis"
      (wcar*
       (with-del-on-start-and-finish {:keys [k]}
         (car/set k v-not-a-set)
         (let [[r1 & rs] (wcar* :as-pipeline
                                (car/spop k))]
           (is (= clojure.lang.ExceptionInfo (type r1)))
           (is (nil? rs))))))))

(deftest when-not-pipelining-exceptions-are-not-immediately-thrown
  ;; We do an exception-throwing Redis command, then some more (non-Redis) work.
  ;; The non-Redis work gets done, and then an exception is thrown.
  ;; Seems a bit strange.
  ;; This strangeness is needed to support the behaviour of throwing exceptions
  ;; when there's a single command but not when there are multiple commands.
  (let [k           "nomis/demo-key-not-a-set"
        v-not-a-set "not-a-set"]
    (wcar*
     (with-del-on-start-and-finish {:keys [k]}
       (car/set k v-not-a-set)
       (let [state        (atom :original-value)
             change-state #(reset! state :changed-value)]
         (is (thrown? Exception
                      (wcar*
                       (car/spop k)   ; Throws exception, later.
                       (change-state) ; This work is done.
                       )))
         (is (= @state :changed-value)))))))

;;;; ___________________________________________________________________________
;;;; Be careful with spelling

(deftest be-careful-with-spelling
  (testing "Might expect an exception here, but no"
    (is (nil? (wcar*
               (car/spop "nomis/demo-key-no-such-key"))))))

;;;; ___________________________________________________________________________
;;;; Be careful with lazy sequences

(deftest avoid-lazy-redis-calls
  (wcar*
   (let [ks ["nomis/demo-key-1"
             "nomis/demo-key-2"]
         v  "my-val"]
     (with-del-on-start-and-finish {:keys ks}
       (testing "Non-lazy -- probably what you want"
         (is (= ["OK" "OK"]
                (car/wcar {} (doseq [k ks] (car/set k v))))))

       (testing "Lazy -- probably not what you want"
         (is (= nil
                (car/wcar {} (for [k ks] (car/set k v))))))))))

;;;; ___________________________________________________________________________
;;;; Clojure data and serialisation

;;;; Clojure strings, keywords and simple numbers are conflated.
;;;; Everything else is auto-serialized and auto-deserialized.

(deftest strings-keywords-and-simple-numbers-are-conflated
  (wcar*

   (testing "For strings, keywords and simple numbers, values come back as strings"
     (let [string-keyword-number ["42" :42 42]
           [k1 k2 k3 :as ks]     ["nomis/demo-key-1"
                                  "nomis/demo-key-2"
                                  "nomis/demo-key-3"]
           [v1 v2 v3]            string-keyword-number
           v-as-string           (first string-keyword-number)]
       (with-del-on-start-and-finish {:keys ks}
         (wcar* (car/set k1 v1)
                (car/set k2 v2)
                (car/set k3 v3))
         (is (= v-as-string (wcar* (car/get k1))))
         (is (= v-as-string (wcar* (car/get k2))))
         (is (= v-as-string (wcar* (car/get k3)))))))

   (testing "For keys, keywords and strings are conflated"
     (let [[v1 v2]        ["my-val-1"
                           "my-val-2"]
           [k1 k2 :as ks] ["nomis/demo"
                           :nomis/demo]]
       (with-del-on-start-and-finish {:keys ks}
         (testing "Set k1/v1"
           (wcar* (car/set k1 v1))
           (testing "k1/v1 got set" (is (= v1 (wcar* (car/get k1)))))
           (testing "k2/v1 got set" (is (= v1 (wcar* (car/get k2))))))
         (testing "Set k2/v2"
           (wcar* (car/set k2 v2))
           (testing "k1/v1 got set" (is (= v2 (wcar* (car/get k1)))))
           (testing "k2/v2 got set" (is (= v2 (wcar* (car/get k2)))))))))))

(deftest serialize-stops-conflation
  (wcar*
   (let [string-keyword-number ["42" :42 42]
         string-keyword-number-each-serialized (map car/serialize
                                                    string-keyword-number)]
     (testing "For values"
       (let [[k1 k2 k3 :as ks] ["nomis/demo-key-1"
                                "nomis/demo-key-2"
                                "nomis/demo-key-3"]
             [sv1 sv2 sv3]     string-keyword-number-each-serialized]
         (with-del-on-start-and-finish {:keys ks}
           (wcar* (car/set k1 sv1)
                  (car/set k2 sv2)
                  (car/set k3 sv3))
           (is (= string-keyword-number
                  (wcar* (car/get k1)
                         (car/get k2)
                         (car/get k3)))))))
     (testing "For keys"
       (let [[k1 k2 k3] string-keyword-number-each-serialized
             [v1 v2 v3] ["my-val-1" "my-val-2" "my-val-3"]]
         (with-del-on-start-and-finish {:keys [k1 k2 k3]}
           (wcar* (car/set k1 v1))
           (wcar* (car/set k2 v2))
           (wcar* (car/set k3 v3))
           (is (= v1 (wcar* (car/get k1))))
           (is (= v2 (wcar* (car/get k2))))
           (is (= v3 (wcar* (car/get k3))))))))))

(def example-clj-data-from-carmine-readme
  {:bigint (bigint 31415926535897932384626433832795)
   :vec    (vec (range 5))
   :set    #{true false :a :b :c :d}
   ;; Don't include `byte-array` here, because "equal" byte arrays
   ;; are not equal:
   ;;     `(= (byte-array 5) (byte-array 5))` => false
   ;; :bytes  (byte-array 5)
   })

(deftest we-can-use-clojure-data-structures-as-values
  (let [v example-clj-data-from-carmine-readme
        k "nomis/demo-key"]
    (wcar*
     (with-del-on-start-and-finish {:keys [k]}
       (car/set k v)
       (is (= v (wcar* (car/get k))))))))

(deftest we-can-use-clojure-data-structures-as-keys
  (let [k example-clj-data-from-carmine-readme
        v "my-val"]
    (wcar*
     (with-del-on-start-and-finish {:keys [k]}
       (wcar* (car/set k v))
       (is (= v (wcar* (car/get k))))))))

(deftest we-can-use-large-unordered-data-structures-as-keys
  (let [k {:nomis/demo (set (range 5000))}
        v "my-val"]
    (wcar* (with-del-on-start-and-finish {:keys [k]}
             (wcar* (car/set k v))
             (is (= v
                    (wcar* (car/get k))))))))

;;;; ___________________________________________________________________________
;;;; Lists

(deftest list-lpush-rpop-test
  (let [vs ["v1" "v2" "v3"]
        k  "nomis/demo-key"]
    (with-del-on-start-and-finish {:keys [k]
                                   :no-error-if-not-exists? true}
      (wcar* (doseq [v vs]
               (car/lpush k v)))
      (is (= vs
             (wcar* (dotimes [_ (count vs)]
                      (car/rpop k))))))))

(deftest we-can-add-multiple-items-to-a-list-in-one-go
  (let [k "nomis/demo-key"]
    (with-del-on-start-and-finish {:keys [k]
                                   :no-error-if-not-exists? true}
      (is (= [[0 0] [0 1] [0 2] [0 3] [0 4]
              [1 0] [1 1] [1 2] [1 3] [1 4]
              [2 0] [2 1] [2 2] [2 3] [2 4]]
             (let [m 3
                   n 5]
               (wcar* (dotimes [i m]
                        (let [vs (for [j (range n)]
                                   [i j])]
                          (apply car/lpush
                                 k
                                 (map car/serialize vs)))))
               (wcar* (dotimes [_ (* m n)]
                        (car/rpop k)))))))))

(deftest clojure-data-and-redis-lists-are-in-different-worlds--obvs
  (let [k "nomis/demo-key"
        v [1 2 3 4]]
    (wcar*
     (with-del-on-start-and-finish {:keys [k]}
       (wcar* (car/set k v))
       (is (thrown-with-msg? Exception
                             #"WRONGTYPE Operation against a key holding the wrong kind of value"
                             (wcar* (car/rpop k))))))))

;;;; ___________________________________________________________________________
;;;; Server-side Lua scripting -- TODO


;;;; ___________________________________________________________________________
;;;; Commands are (just) functions

(deftest cammands-are-functions

  (testing "`car/ping` is a function"
    (is (= ["PONG" "PONG" "PONG" "PONG" "PONG"]
           (wcar* (doall (repeatedly 5 car/ping))))))

  (testing "We can nest `car/wcar` calls"
    (is (= ["OK" "Hickey" "Sanfilippo"]
           (let [hash-key "awesome-people"]
             (wcar* (car/hmset hash-key
                               "Rich" "Hickey"
                               "Salvatore" "Sanfilippo")
                    (mapv (partial car/hget hash-key)
                          ;; Execute with own connection & pipeline then
                          ;; return result for composition:
                          (wcar* (car/hkeys hash-key)))))))))

;;;; ___________________________________________________________________________
;;;; Listeners & Pub/Sub -- TODO


;;;; ___________________________________________________________________________
;;;; Reply parsing -- TODO

;;;; ___________________________________________________________________________
;;;; Binary data -- TODO

;;;; ___________________________________________________________________________
;;;; Message queue -- TODO

;;;; TODO Look at pools and message queues

;;;; ___________________________________________________________________________
;;;; Distributed locks -- TODO

;;;; ___________________________________________________________________________
;;;; Tundra (beta) -- TODO
