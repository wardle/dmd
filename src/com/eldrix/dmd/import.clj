; Copyright 2021 Mark Wardle and Eldrix Ltd
;
;   Licensed under the Apache License, Version 2.0 (the "License");
;   you may not use this file except in compliance with the License.
;   You may obtain a copy of the License at
;
;       http://www.apache.org/licenses/LICENSE-2.0
;
;   Unless required by applicable law or agreed to in writing, software
;   distributed under the License is distributed on an "AS IS" BASIS,
;   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;   See the License for the specific language governing permissions and
;   limitations under the License.
;;;;
(ns com.eldrix.dmd.import
  "Support the UK NHS dm+d XML data files.
  This namespace provides a thin wrapper over the data files, keeping the
  original structures as much as possible and thus facilitating adapting
  to changes in those definitions as they occur.

  For more information see
  https://www.nhsbsa.nhs.uk/sites/default/files/2017-02/Technical_Specification_of_data_files_R2_v3.1_May_2015.pdf"
  (:require [clojure.core.async :as a]
            [clojure.core.match :refer [match]]
            [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zx]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging.readable :as log]
            [clojure.zip :as zip])
  (:import [java.time LocalDate]
           (java.time.format DateTimeFormatter DateTimeParseException)))

;; dm+d date format = CCYY-MM-DD
(defn- ^LocalDate parse-date [^String s] (try (LocalDate/parse s (DateTimeFormatter/ISO_LOCAL_DATE)) (catch DateTimeParseException _)))
(defn- ^Long parse-long [^String s] (Long/parseLong s))
(defn- ^Boolean parse-flag [^String s] (= "1" s))


(def ^:private file-ordering
  "Order of file import for relational integrity, if needed."
  [:LOOKUP :INGREDIENT :VTM :VMP :AMP :VMPP :AMPP])

(def ^DateTimeFormatter df
  (DateTimeFormatter/ofPattern "ddMMyy"))

(def ^:private file-matcher
  "There is no formal specification for filename structure, but this is the de
  facto standard."
  #"^f_([a-z]*)2_\d(\d{6})\.xml$")

(defn ^:private parse-dmd-filename
  "Parse a dm+d filename if possible."
  [f]
  (let [f2 (clojure.java.io/as-file f)]
    (when-let [[_ nm date] (re-matches file-matcher (.getName f2))]
      (let [kw (keyword (str/upper-case nm))]
        {:type  kw
         :date  (LocalDate/parse date df)
         :order (.indexOf file-ordering kw)
         :file  f2}))))

(defn should-include?
  [include exclude file-type]
  (if (or (nil? include) (contains? include file-type))
    (not (contains? exclude file-type))))

(defn dmd-file-seq
  "Return an ordered sequence of dm+d files from the directory specified.
  Components are returned in an order to support referential integrity.
  Each result is a map containing :type, :date, :order and :file.
  Optionally takes a set of file types to include or exclude."
  ([dir]
   (->> dir
        clojure.java.io/file
        file-seq
        (map parse-dmd-filename)
        (filter some?)
        (sort-by :order)))
  ([dir & {:keys [include exclude]}]
   (filter #(should-include? include exclude (:type %)) (dmd-file-seq dir))))

(defn get-release-metadata
  "Return release metadata from the directory specified.
  Unfortunately, the dm+d distribution does not include a metadata file
  containing release information, so version information is derived from
  the filenames within the release.

  Parameters:
   - dir   : directory to examine

  Result:
   - a map containing release information:
     |- :release-date   - date of the release (java.time.LocalDate)

  As far as I am aware, there is no formal specification for  dm+d filenames,
  but currently the last six digits of the filename are a date of format
  'ddMMyy' so we use the latest date as the date of the release."
  [dir]
  (when-let [release-date (last (sort (map :date (dmd-file-seq dir))))]
    {:release-date release-date}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Generic dm+d parsing functionality
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private property-parsers
  {[[:LOOKUP :UNIT_OF_MEASURE] :CD]     parse-long
   [[:LOOKUP :UNIT_OF_MEASURE] :CDPREV] parse-long
   [[:VMP :FORM] :CD]                   parse-long
   [[:VMP :FORM] :CDPREV]               parse-long
   [[:VMP :ROUTE] :CD]                  parse-long
   [[:VMP :ROUTE] :CDPREV]              parse-long
   [[:VMP :DRUG_FORM] :FORMCD]          parse-long
   [[:LOOKUP :SUPPLIER] :CD]            parse-long
   [[:LOOKUP :SUPPLIER] :CDPREV]        parse-long
   :CDDT                                parse-date
   :VTMID                               parse-long
   :VTMIDPREV                           parse-long
   :INVALID                             parse-flag
   :SUG_F                               parse-flag
   :GLU_F                               parse-flag
   :PRES_F                              parse-flag
   :CFC_F                               parse-flag
   :VTMIDDT                             parse-date
   :VPID                                parse-long
   :VPIDPREV                            parse-long
   :UDFS                                edn/read-string
   :UDFS_UOMCD                          parse-long
   :UNIT_DOSE_UOMCD                     parse-long
   :ISID                                parse-long
   :ISIDPREV                            parse-long
   :ISIDDT                              parse-date
   :BS_SUBID                            parse-long
   :STRNT_NMRTR_VAL                     edn/read-string
   :STRNT_NMRTR_UOMCD                   parse-long
   :STRNT_DNMTR_VAL                     edn/read-string
   :STRNT_DNMTR_UOMCD                   parse-long
   :ROUTECD                             parse-long
   :CATDT                               parse-date
   :NMDT                                parse-date
   :SUPPCD                              parse-long
   :APID                                parse-long
   :LIC_AUTHCHANGEDT                    parse-date
   :VPPID                               parse-long
   :QTYVAL                              edn/read-string
   :QTY_UOMCD                           parse-long
   :APPID                               parse-long
   :REIMB_STATDT                        parse-date
   :DISCDT                              parse-date
   :PRNTVPPID                           parse-long
   :CHLDVPPID                           parse-long
   :PRNTAPPID                           parse-long
   :CHLDAPPID                           parse-long})

(defn- parse-property [kind kw v]
  (if-let [parser (get property-parsers [kind kw])]
    {kw (parser v)}
    (if-let [fallback (get property-parsers kw)]
      {kw (fallback v)}
      {kw v})))

(defn- parse-dmd-component
  "Parse a fragment of XML.
  Does not process nested XML but that is not required for the dm+d XML."
  ([node] (parse-dmd-component nil node))
  ([kind node]
   (reduce into (if kind {:TYPE kind} {})
           (map #(parse-property kind (:tag %) (first (:content %))) (:content node)))))

(defn- stream-flat-dmd
  "Streams dm+d components from a flat XML file; blocking.
  This expects top-level tags to represent the components themselves.
  Suitable for parsing dm+d VTM and INGREDIENT file.

  Each component has TYPE information added in the form
  [file-type component-type].

  For example: `[:VTM :VTM]`"
  [root ch file-type close?]
  (let [kind [file-type file-type]]
    (a/<!! (a/onto-chan!! ch (map (partial parse-dmd-component kind) (:content root)) close?))
    (when close? (a/close! ch))))

(defn- stream-nested-dmd
  "Stream dm+d components from a nested dm+d distribution file; blocking.
  A nested file contains multiple components; for example VMP contains VMPS
  as well as VIRTUAL_PRODUCT_INGREDIENT and ONT_DRUG_FORM. Unfortunately
  the naming is inconsistent with some in the plural and some in the singular.

  Each component has TYPE information added in the form
  [file-type component-type].
  For example: `[:VMP :DRUG_FORM]`"
  [root ch file-type close?]
  (loop [components (:content root)]
    (when-let [component (first components)]
      (let [[_ subtag] (first component)
            subtag' (if (= (str (name file-type) "S") (name subtag)) file-type subtag) ;; fix inconsistent naming of plural components
            kind [file-type subtag']]
        (a/<!! (a/onto-chan!! ch (map (partial parse-dmd-component kind) (:content component)) false)))
      (recur (next components))))
  (when close? (a/close! ch)))

(defn- stream-lookup-xml
  [root ch file-type close?]
  (loop [lookups (:content root)]
    (if-let [lookup (first lookups)]
      (let [tag (:tag lookup)
            result (->> (:content lookup)
                        (map (partial parse-dmd-component [file-type tag]))
                        (map #(assoc % :ID (keyword (str (name tag) "-" (:CD %))))))]
        (a/<!! (a/onto-chan!! ch result false))
        (recur (next lookups)))
      (when close? (a/close! ch)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; High-level dm+d processing functionality
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:private streamers
  {:VTM        stream-flat-dmd
   :VMP        stream-nested-dmd
   :AMP        stream-nested-dmd
   :VMPP       stream-nested-dmd
   :AMPP       stream-nested-dmd
   :INGREDIENT stream-flat-dmd
   :LOOKUP     stream-lookup-xml})

(defn- stream-dmd-file [ch close? {:keys [type file] :as dmd-file}]
  (log/info "Processing " dmd-file)
  (if-let [streamer (get streamers type)]
    (with-open [rdr (io/reader file)]
      (let [root (xml/parse rdr :skip-whitespace true)]
        (streamer root ch type close?)))
    (log/info "Skipping file " dmd-file)))

(defn stream-dmd
  "Streams dm+d components from the directory to the channel.
  Components are ordered to maintain relational integrity should it be required.
  This does minimal processing; streamed data is a close representation of the
  dm+d data structures.Some properties are parsed such as dates (xxxDT) to a
  java.time.LocalDate, flags (e.g. invalidity) to booleans, and identifiers to
  'long's. Each component is labelled with its type as :TYPE. This is a tuple of
  file and entity: e.g. [:VMP :DRUG_FORM].

  Parameters:
  - dir      : directory containing dm+d distribution
  - ch       : clojure.core.async channel
  - ordered? : whether components should be streamed in order, default true
  - include  : a set of dm+d file types to include (e.g. #{:VTM})
  - exclude  : a set of dm+d file types to exclude (e.g. #{:VTM})"
  [dir ch & {:keys [ordered? _include _exclude] :or {ordered? true} :as opts}]
  (log/info "Importing from " dir)
  (let [files (dmd-file-seq dir opts)]
    (if ordered?
      ;; simple ordered processing; do one by one
      (doseq [dmd-file files]
        (stream-dmd-file ch false dmd-file))
      ;; parallel processing - process each file in separate thread
      (loop [files' files
             done-chs []]
        (let [dmd-file (first files')]
          (if-not dmd-file
            (a/<!! (a/merge done-chs))                      ;; when all files scheduled, wait for all to complete
            (recur (next files')
                   (conj done-chs (a/thread (stream-dmd-file ch false dmd-file))))))))
    ;; finally, close channel when we're done
    (a/close! ch)))

(defn statistics-dmd
  "Return statistics for dm+d data in the specified directory."
  [dir]
  (let [ch (a/chan)]
    (a/thread (stream-dmd dir ch))
    (loop [item (a/<!! ch)
           counts {}]
      (if-not item
        counts
        (recur (a/<!! ch)
               (update counts (:TYPE item) (fnil inc 0)))))))

(defn- ch->seq*
  [ch]
  (when-let [item (a/<!! ch)]
    (cons item (lazy-seq (ch->seq* ch)))))

(defn ch->seq
  "Turns a clojure core.async channel into a lazy sequence."
  [ch]
  (lazy-seq (ch->seq* ch)))

(defn get-component
  "Convenience function to stream only the specified component.
  Useful for testing.
  Parameters:
  - dir            : directory from which to load dm+d files
  - file-type      : dm+d type  e.g. :LOOKUP
  - component-type : component type e.g. :COMBINATION_PACK_IND

  `file-type` and `component-type` are keywords representing the names from the
  dm+d specification.
  :VTM :VTM   - returns all VTMs
  :VMP :VMP   - returns all VMPs
  :VMP :ONT_DRUG_FORM - returns all ONT_DRUG_FORMS from the VMP file."
  [dir file-type component-type]
  (let [ch (a/chan 200 (filter #(= [file-type component-type] (:TYPE %))))]
    (a/thread (stream-dmd dir ch :include #{file-type}))
    (ch->seq ch)))

(comment
  (dmd-file-seq "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001")
  (dmd-file-seq "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :include #{:VTM :VMP} :exclude #{:VMP})
  (dmd-file-seq "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :include #{:VTM})
  (dmd-file-seq "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :exclude #{:VTM})
  (get-release-metadata "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001")
  (get-release-metadata "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001")
  (statistics-dmd "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001")

  (get-component "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :LOOKUP :LEGAL_CATEGORY)
  (get-component "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :VTM :VTM)
  (get-component "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :VMP :VMP)
  (get-component "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :AMP :AP_INGREDIENT)

  )
