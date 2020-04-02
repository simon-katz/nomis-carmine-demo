(ns com.nomistech.nomis-redis-carmine-demo.code-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [com.nomistech.nomis-redis-carmine-demo.code :as sut]
            [taoensso.carmine :as car]))

;;;; NB: This all assumes that we have Redis running on redis://localhost:6379.

;;;; Notes adapted from https://github.com/ptaoussanis/carmine, with extra bits.

;;;; ___________________________________________________________________________
;;;; Connection

(def my-conn-spec
  {:pool {}
   :spec {:uri "redis://localhost:6379"}})

;;;; ___________________________________________________________________________
;;;; Fixtures

(use-fixtures
  :once
  (fn [f]
    (car/wcar my-conn-spec
              (car/set "nomis/not-a-set" "a-simple-value"))
    (f)
    (car/wcar my-conn-spec
              (car/del "nomis/not-a-set"))))

;;;; ___________________________________________________________________________
;;;; Basics

(deftest ping-test
  (is (= "PONG"
         (car/wcar my-conn-spec
                   (car/ping)))))

(deftest set-and-get-test
  (try (is (= "OK"
              (car/wcar my-conn-spec
                        (car/set "nomis/foo" "bar"))))
       (is (= "bar"
              (car/wcar my-conn-spec
                        (car/get "nomis/foo"))))
       (finally
         (car/wcar my-conn-spec
                   (car/del "nomis/foo")))))

(deftest ping-set-and-get-test
  (testing "If we execute multiple Redis commands, we get a vector of results"
    (try (is (= ["PONG" "OK" "bar"]
                (car/wcar my-conn-spec
                          (car/ping)
                          (car/set "nomis/foo" "bar")
                          (car/get "nomis/foo"))))
         (finally
           (car/wcar my-conn-spec
                     (car/del "nomis/foo"))))))

(deftest about-vectors-or-not-as-the-result

  ;; This is about what the use, or not, of Redis pipelines (for which,
  ;; see https://redis.io/topics/pipelining).
  ;;
  ;; When we use a pipeline, we get a vector of results.

  (let [ping-n-times (fn [n] (dotimes [_ n] (car/ping)))]

    (testing "Without `:as-pipeline`"

      (testing "A single command returns a single result, not in a vector"
        (is (= "PONG"
               (car/wcar my-conn-spec (ping-n-times 1)))))

      (testing "Multiple commands return a vector of results"
        ;; TODO It isn't clear to me what exactly is going on here.
        ;;      At what point does Carmine decide we are pipelining?
        ;;      Is everything always executed in a pipeline? Or does Carmine
        ;;      start a pipeline for the second and subsequent commands?
        (is (= ["PONG" "PONG" "PONG" "PONG"]
               (car/wcar my-conn-spec (ping-n-times 4))))))

    (testing "With `:as-pipeline`"

      (testing "A single command returns a singleton vector result"
        (is (= ["PONG"]
               (car/wcar my-conn-spec :as-pipeline (ping-n-times 1)))))

      (testing "Multiple commands return a vector of results"
        (is (= ["PONG" "PONG" "PONG" "PONG"]
               (car/wcar my-conn-spec :as-pipeline (ping-n-times 4))))))))

(deftest be-careful--we-can-have-vectors-as-values
  (try (is (= "OK"
              (car/wcar my-conn-spec
                        (car/set "nomis/foo" [1 2 3]))))
       (testing "We can have a vector value -- so testing for vectors is not a general solution to seeing whether pipelining happened"
        (is (= [1 2 3]
               (car/wcar my-conn-spec
                         (car/get "nomis/foo")))))
       (finally
         (car/wcar my-conn-spec
                   (car/del "nomis/foo")))))

(deftest throws-exception-on-redis-error-unless-pipelining

  (testing "Exception when not pipelining"
    (is (thrown? Exception
                 (car/wcar my-conn-spec
                           (car/spop "nomis/not-a-set")))))

  (testing "No exception when pipelining"
    (let [[r1 r2 & rs] (car/wcar my-conn-spec
                                 (car/set "nomis/not-a-set" "bar")
                                 (car/spop "nomis/not-a-set"))]
      (is (= "OK" r1))
      (is (= clojure.lang.ExceptionInfo
             (type r2)))
      (is (= {:prefix :wrongtype}
             (ex-data r2)))
      (is (nil? rs))))

  (testing "No exception when pipelining, even when only one call to Redis"
    (let [[r1 & rs] (car/wcar my-conn-spec
                              :as-pipeline
                              (car/spop "nomis/not-a-set"))]
      (is (= clojure.lang.ExceptionInfo (type r1)))
      (is (nil? rs)))))

(deftest when-not-pipelining-exceptions-are-not-immediately-thrown
  ;; We do an exception-throwing Redis command, then some more (non-Redis) work.
  ;; The non-Redis work gets done, and then an exception is thrown.
  ;; Seems a bit strange.
  ;; This strangeness is needed to support the behaviour of throwing exceptions
  ;; when there's a single command but not when there are multiple commands.
  (let [state (atom :original-value)
        change-state #(reset! state :changed-value)]
    (is (thrown? Exception
                 (car/wcar my-conn-spec
                           (car/spop "nomis/not-a-set") ; => exception, later
                           (change-state) ; this work is done
                           )))
    (is (= @state :changed-value))))

;;;; ___________________________________________________________________________

(deftest be-careful-with-spelling
  (testing "Might expect an exception here, but no"
    (is (nil? (car/wcar my-conn-spec
                        (car/spop "nomis/not-a-set-misspelled!!!!!"))))))

;;;; ___________________________________________________________________________
;;;; Be careful with lazy sequences

(deftest avoid-lazy-redis-calls

  (testing "Non-lazy -- probably what you want"
    (is (= ["OK" "OK"]
           (car/wcar {} (doseq [k [:k1 :k2]] (car/set k :val))))))

  (testing "Lazy -- probably not what you want"
    (is (= nil
           (car/wcar {} (for [k [:k1 :k2]] (car/set k :val)))))))

;;;; ___________________________________________________________________________
;;;; Key names

(deftest about-key-names
  (testing "We can use Clojure data structures as keys"
    (let [test-key (fn [key]
                     (try (is (= [key ["OK" "bar"]]
                                 [key (car/wcar my-conn-spec
                                                (car/set key "bar")
                                                (car/get key))]))
                          (finally
                            (car/wcar my-conn-spec
                                      (car/del key)))))]
      (test-key "nomis/foo")
      (test-key :nomis/foo)
      (test-key 'nomis/foo)
      (test-key [:a :b])
      (test-key {:a 1
                 :b [1 2 #{4 5}]}))))

;;;; ___________________________________________________________________________
;;;; Data serialisation

(deftest data-serialisation-test
  (let [data {:bigint (bigint 31415926535897932384626433832795)
              :vec    (vec (range 5))
              :set    #{true false :a :b :c :d}
              ;; Don't include `byte-array` here, because "equal" byte arrays
              ;; are not equal:
              ;;     `(= (byte-array 5) (byte-array 5))` => false
              ;; :bytes  (byte-array 5)
              }]
    (is (= ["OK" data]
           (car/wcar my-conn-spec
                     (car/set "clj-key" data)
                     (car/get "clj-key"))))))
;;;; ___________________________________________________________________________
;;;; Server-side Lua scripting -- TODO


;;;; ___________________________________________________________________________
;;;; Commands are (just) functions

(deftest cammands-are-functions

  (testing "`car/ping` is a function"
    (is (= ["PONG" "PONG" "PONG" "PONG" "PONG"]
           (car/wcar my-conn-spec
                     (doall (repeatedly 5 car/ping))))))

  (testing "We can nest `car/wcar` calls"
    (is (= ["OK" "Hickey" "Sanfilippo"]
           (let [hash-key "awesome-people"]
             (car/wcar my-conn-spec
                       (car/hmset hash-key
                                  "Rich" "Hickey"
                                  "Salvatore" "Sanfilippo")
                       (mapv (partial car/hget hash-key)
                             ;; Execute with own connection & pipeline then
                             ;; return result for composition:
                             (car/wcar my-conn-spec (car/hkeys hash-key)))))))))

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
