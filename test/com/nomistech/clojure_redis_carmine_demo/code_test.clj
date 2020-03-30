(ns com.nomistech.clojure-redis-carmine-demo.code-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [com.nomistech.clojure-redis-carmine-demo.code :as sut]
            [taoensso.carmine :as car]))

(def my-conn-spec
  {:pool {}
   :spec {:uri "redis://127.0.0.1:6379"}})

;;;; ___________________________________________________________________________
;;;; Basics

(deftest test-001
  (is (= "PONG"
         (car/wcar my-conn-spec
                   (car/ping)))))

(deftest test-002
  (is (= ["PONG" "OK" "bar"]
         (car/wcar my-conn-spec
                   (car/ping)
                   (car/set "foo" "bar")
                   (car/get "foo")))))

(deftest test-003-not-a-proper-body-and-inconsistent
  ;; Hmmmm.
  ;; `car/wcar` seems to be badly designed.
  ;; - The body is not a proper body -- the result is not the result of the
  ;;   last form in the body.
  ;; - Vector of results, or not:
  ;;   - It collects up results of Redis calls into a vector.
  ;;   - You don't get a vector if there is only one Redis call.
  ;;   - But `:as-pipeline` lets you fiddle.

  (let [n-pings (fn [n] (dotimes [_ n] (car/ping)))]

    (testing "Without `:as-pipeline`"
      (is (= "PONG"
             (car/wcar my-conn-spec (n-pings 1))))
      (is (= ["PONG" "PONG" "PONG" "PONG"]
             (car/wcar my-conn-spec (n-pings 4)))))

    (testing "With `:as-pipeline`"
      (is (= ["PONG"]
             (car/wcar my-conn-spec :as-pipeline (n-pings 1))))
      (is (= ["PONG" "PONG" "PONG" "PONG"]
             (car/wcar my-conn-spec :as-pipeline (n-pings 4)))))))

(deftest throws-exception-on-redis-error-unless-pipelining

  (testing "xxxx"
    (car/wcar my-conn-spec
              (car/set "foo" "bar"))
    (is (thrown? Exception
                 (car/wcar my-conn-spec
                           (car/spop "foo")))))

  (testing "but no exception when pipelining" ; Hmmmm.
    (let [result (car/wcar my-conn-spec
                           (car/set "foo" "bar")
                           (car/spop "foo"))]
      ;; TODO Look into pattern matching with clojure.test.
      (is (= 2
             (count result)))
      (is (= "OK"
             (first result)))
      (is (= clojure.lang.ExceptionInfo
             (type (second result))))
      (is (= {:prefix :wrongtype}
             (ex-data (second result)))))) )

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
