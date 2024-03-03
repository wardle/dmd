(ns build
  (:require [borkdude.gh-release-artifact :as gh]
            [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'com.eldrix/dmd)
(def version (format "1.0.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def jar-basis (b/create-basis {:project "deps.edn"}))
(def uber-basis (b/create-basis {:project "deps.edn"
                                 :aliases [:run]}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def uber-file (format "target/%s-server-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (clean nil)
  (println "Building" lib version)
  (b/write-pom {:class-dir class-dir
                :lib       lib
                :version   version
                :basis     jar-basis
                :src-dirs  ["src"]
                :scm       {:url                 "https://github.com/wardle/dmd"
                            :tag                 (str "v" version)
                            :connection          "scm:git:git://github.com/wardle/dmd.git"
                            :developerConnection "scm:git:ssh://git@github.com/wardle/dmd.git"}
                :pom-data  [[:description "Implementation of the UK NHS dictionary of medicines and devices (dm+d)."]
                            [:developers
                             [:developer
                              [:id "wardle"] [:name "Mark Wardle"] [:email "mark@wardle.org"] [:url "https://wardle.org"]]]
                            [:organization [:name "Eldrix Ltd"]]
                            [:licenses
                             [:license
                              [:name "Eclipse Public License v2.0"]
                              [:url "https://www.eclipse.org/org/documents/epl-2.0/EPL-2.0.html"]
                              [:distribution "repo"]]]]})
  (b/copy-dir {:src-dirs   ["src"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file  jar-file}))

(defn install
  "Installs pom and library jar in local maven repository"
  [_]
  (jar nil)
  (println "Installing" lib version)
  (b/install {:basis     jar-basis
              :lib       lib
              :class-dir class-dir
              :version   version
              :jar-file  jar-file}))

(defn deploy
  "Deploy library to clojars.
  Environment variables CLOJARS_USERNAME and CLOJARS_PASSWORD must be set."
  [_]
  (clean nil)
  (jar nil)
  (dd/deploy {:installer :remote
              :artifact  jar-file
              :pom-file  (b/pom-path {:lib       lib
                                      :class-dir class-dir})}))
(defn uber
  "Build an executable uberjar file for hermes HTTP server"
  [_]
  (clean nil)
  (b/copy-dir {:src-dirs   ["src" "cmd" "resources"]
               :target-dir class-dir})
  (b/compile-clj {:basis      uber-basis
                  :src-dirs   ["src" "cmd"]
                  :ns-compile ['com.eldrix.dmd.cli]
                  :class-dir  class-dir})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis     uber-basis
           :main      'com.eldrix.dmd.cli}))

(defn release
  "Deploy release to GitHub. Requires valid token in GITHUB_TOKEN environmental
       variable."
  [_]
  (uber nil)
  (println "Deploying release to GitHub")
  (gh/release-artifact {:org    "wardle"
                        :repo   "dmd"
                        :tag    (str "v" version)
                        :file   uber-file
                        :sha256 true}))
