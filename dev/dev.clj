(ns dev
  (:require [clojure.java.classpath :as classpath]
            [clojure.java.javadoc :refer [javadoc]]
            [clojure.pprint :refer [pp pprint]]
            [clojure.repl :refer [apropos dir doc find-doc pst source]]
            [clojure.tools.namespace.repl :as tnr])
  (:import (java.io File)))

;;;; ___________________________________________________________________________
;;;; Fix classpath directories for tools.namspace.

;;;; I have found that `refresh` does nothing.
;;;; It seems:
;;;; - `(classpath/classpath)` does not end up calling
;;;;   `(classpath/system-classpath)`.
;;;; - That's because
;;;;   `(classpath/classpath (clojure.lang.RT/baseLoader))`
;;;;   returns a File for "/Library/Java/JavaVirtualMachines/adoptopenjdk-13.0.1.jdk/Contents/Home/lib/src.zip".
;;;; - I've never seen this problem before.
;;;;   Might it be related to jenv? (Just a guess.)

(let [dirs-on-classpath (filter #(.isDirectory ^File %)
                                (classpath/system-classpath))]
  (apply tnr/set-refresh-dirs
         dirs-on-classpath))
