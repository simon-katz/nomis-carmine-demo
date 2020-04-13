(ns dev
  (:require [clojure.main]
            [clojure.tools.namespace.repl :as tnr]))

;;;; ___________________________________________________________________________
;;;; Require the standard REPL utils.
;;;;
;;;; This is useful in a `dev` namespace, and is needed in a `user` namespace
;;;; because the requires get blatted by `tnr/refresh` and `reset`.

(apply require clojure.main/repl-requires)

;;;; ___________________________________________________________________________

;;;; `clojure.tools.namespace.repl/refresh` sometimes doesn't work, because it
;;;; doesn't know what directories to read from when refreshing. This is a
;;;; problem caused by different behaviour around classpaths on different
;;;; versions of the JVM.
;;;;
;;;; See:
;;;; - https://ask.clojure.org/index.php/8288/java-11-and-tools-namespace
;;;; - https://github.com/clojure-emacs/cider/issues/2686
;;;; - https://github.com/clojure-emacs/cider/issues/2686#issuecomment-532389499
;;;;
;;;; Conclusion:
;;;; - Do the following:

(tnr/set-refresh-dirs "dev" "src" "test")
