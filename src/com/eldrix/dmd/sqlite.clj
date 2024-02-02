(ns com.eldrix.dmd.sqlite
  (:import (java.util.regex Pattern)))

(gen-class
  :name com.eldrix.dmd.sqlite.Regexp
  :prefix "regexp-"
  :extends org.sqlite.Function
  :exposes-methods {result superResult, value_text superValueText})

(defn regexp-xFunc
  [this]
  (let [expression (.superValueText this 0)
        value (or (.superValueText this 1) "")
        pattern (Pattern/compile expression)
        matcher (.matcher pattern value)]
    (.superResult this (if (.find matcher) 1 0))))

(comment
  (compile 'com.eldrix.dmd.sqlite))