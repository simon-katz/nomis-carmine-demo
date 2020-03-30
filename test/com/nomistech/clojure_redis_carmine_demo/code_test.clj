(ns com.nomistech.clojure-redis-carmine-demo.code-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [com.nomistech.clojure-redis-carmine-demo.code :as sut]
            [taoensso.carmine :as car]))

(def my-conn-spec
  {:pool {}
   :spec {:uri "redis://127.0.0.1:6379"}})

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

(deftest test-003
  ;; Grrrr!
  ;; `car/wcar` is badly designed.
  ;; - The body is not a proper body -- the result is not the result of the
  ;;   last form in the body.
  ;; - It collects up results of Redis calls into a vector.
  ;; - You don't get a vector if there is only one Redis call.
  (for [n [1 4]]
    (car/wcar my-conn-spec
              (dotimes [i n] (car/ping)))))

