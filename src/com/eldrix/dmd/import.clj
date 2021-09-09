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
  https://www.nhsbsa.nhs.uk/sites/default/files/2021-02/dm%2Bd%20data%20model%20%28V2%29%2002.2021.pdf
  https://www.nhsbsa.nhs.uk/sites/default/files/2017-02/Technical_Specification_of_data_files_R2_v3.1_May_2015.pdf
  https://www.nhsbsa.nhs.uk/sites/default/files/2017-02/Data_Model_R2_v3.1_May_2015.pdf"
  (:require [clojure.core.async :as a]
            [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zx]
            [clojure.java.io :as io]
            [clojure.pprint]
            [clojure.string :as str]
            [clojure.tools.logging.readable :as log]
            [clojure.zip])
  (:import [java.time LocalDate]
           (java.time.format DateTimeFormatter DateTimeParseException)
           (java.util List)))

(set! *warn-on-reflection* true)

;; dm+d date format = CCYY-MM-DD
(defn- ^LocalDate parse-date [^String s] (try (LocalDate/parse s (DateTimeFormatter/ISO_LOCAL_DATE)) (catch DateTimeParseException _)))
(defn- ^Long parse-long [^String s] (Long/parseLong s))
(defn- ^Integer parse-integer [^String s] (Integer/parseInt s))
(defn- ^Boolean parse-flag [^String s] (boolean (= 1 (Integer/parseInt s)))) ;; just for fun, they sometimes use "1" or "0001" for flags...
(defn- ^Double parse-double [^String s] (Double/parseDouble s))

(def ^:private file-ordering
  "Order of file import for relational integrity, if needed."
  [:LOOKUP :INGREDIENT :VTM :VMP :AMP :VMPP :AMPP :GTIN :BNF])

(def ^DateTimeFormatter df
  (DateTimeFormatter/ofPattern "ddMMyy"))

(def ^:private file-matcher
  "There is no formal specification for filename structure, but this is the de
  facto standard."
  #"^f_([a-z]*)\d_\d(\d{6})\.xml$")

(defn ^:private parse-dmd-filename
  "Parse a dm+d filename if possible."
  [f]
  (let [f2 (clojure.java.io/as-file f)]
    (when-let [[_ nm date] (re-matches file-matcher (.getName f2))]
      (let [kw (keyword (str/upper-case nm))]
        {:type  kw
         :date  (LocalDate/parse date df)
         :order (.indexOf ^List file-ordering kw)
         :file  f2}))))

(defn should-include?
  [include exclude file-type]
  (when (or (nil? include) (contains? include file-type))
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
  {
   ;; VTM properties
   :VTMID             parse-long
   :INVALID           parse-flag
   :VTMIDPREV         parse-long
   :VTMIDDT           parse-date

   ;; VMP properties
   :VPID              parse-long
   :VPIDDT            parse-date
   :VPIDPREV          parse-long
   :BASISCD           parse-long
   :NMDT              parse-date
   :BASIS_PREVCD      parse-long
   :NMCHANGECD        parse-long
   :COMBPRODCD        parse-long
   :PRES_STATCD       parse-long
   :SUG_F             parse-flag
   :GLU_F             parse-flag
   :PRES_F            parse-flag
   :CFC_F             parse-flag
   :NON_AVAILCD       parse-long
   :NON_AVAILDT       parse-date
   :DF_INDCD          parse-long
   :UDFS              parse-double
   :UDFS_UOMCD        parse-long
   :UNIT_DOSE_UOMCD   parse-long
   :ISID              parse-long
   :BASIS_STRNTCD     parse-long
   :BS_SUBID          parse-long
   :STRNT_NMRTR_VAL   parse-double
   :STRNT_NMRTR_UOMCD parse-long
   :STRNT_DNMTR_VAL   parse-double
   :STRNT_DNMTR_UOMCD parse-long
   :FORMCD            parse-long
   :ROUTECD           parse-long
   :CATCD             parse-long
   :CATDT             parse-date
   :CAT_PREVCD        parse-long

   ;; AMP properties
   :APID              parse-long
   :SUPPCD            parse-long
   :LIC_AUTHCD        parse-long
   :LIC_AUTH_PREVCD   parse-long
   :LIC_AUTHCHANGECD  parse-long
   :LIC_AUTHCHANGEDT  parse-date
   :FLAVOURCD         parse-long
   :EMA               parse-flag
   :PARALLEL_IMPORT   parse-flag
   :AVAIL_RESTRICTCD  parse-long
   :STRNTH            parse-double
   :UOMCD             parse-long
   :COLOURCD          parse-long

   ;; VMPP properties
   :VPPID             parse-long
   :QTYVAL            parse-double
   :QTY_UOMCD         parse-long
   :COMBPACKCD        parse-long
   :PAY_CATCD         parse-long
   :PRICE             parse-integer
   :DT                parse-date
   :PREVPRICE         parse-integer
   :PRNTVPPID         parse-long
   :CHLDVPPID         parse-long

   ;; AMPP properties
   :APPID             parse-long
   :LEGAL_CATCD       parse-long
   :DISCCD            parse-long
   :DISCDT            parse-date
   :REIMB_STATCD      parse-long
   :REIMB_STATDT      parse-date
   :REIMB_STATPREVCD  parse-long
   :SCHED_2           parse-flag
   :ACBS              parse-flag
   :PADM              parse-flag
   :FP10_MDA          parse-flag
   :SCHED_1           parse-flag
   :HOSP              parse-flag
   :NURSE_F           parse-flag
   :ENURSE_F          parse-flag
   :DENT_F            parse-flag
   :PRICEDT           parse-date
   :PRICE_BASISCD     parse-long
   :PX_CHRGS          parse-flag
   :DISP_FEES         parse-flag                            ;; unlike the documentation, this is actually a flag (1 or omitted).
   :BB                parse-flag
   :CAL_PACK          parse-flag
   :SPEC_CONTCD       parse-long
   :FP34D             parse-flag
   :PRNTAPPID         parse-long
   :CHLDAPPID         parse-long

   ;; ingredients
   :ISIDPREV          parse-long
   :ISIDDT            parse-date

   ;; lookups
   :CD                parse-long
   :CDPREV            parse-long
   :CDDT              parse-date

   ;; BNF / extras
   :DDD_UOMCD         parse-long
   :DDD               parse-double
   :STARTDT           parse-date
   :ENDDT             parse-date

   })

(defn- parse-property [k v]
  (if-let [parser (get property-parsers k)]
    {k (parser v)}
    {k v}))

(defn- parse-dmd-component
  "Parse a fragment of XML.
  Does not process nested XML but that is not required for the dm+d XML."
  ([node] (parse-dmd-component nil node))
  ([kind node]
   (reduce into (if kind {:TYPE kind} {})
           (map #(parse-property (:tag %) (first (:content %))) (:content node)))))

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
                        (map (partial parse-dmd-component [file-type tag])))]
        (a/<!! (a/onto-chan!! ch result false))
        (recur (next lookups)))
      (when close? (a/close! ch)))))

(defn parse-gtin
  "Note: unlike other AMPP related components, this uses :AMPPID as the key!"
  [loc]
  (let [gtin (zx/xml1-> loc :GTINDATA :GTIN zx/text)
        startdt (zx/xml1-> loc :GTINDATA :STARTDT zx/text)
        enddt (zx/xml1-> loc :GTINDATA :ENDDT zx/text)]
    (cond-> {:TYPE   [:GTIN :AMPP]
             :AMPPID (Long/parseLong (zx/xml1-> loc :AMPPID zx/text))}
            gtin
            (assoc :GTIN (Long/parseLong gtin))
            startdt
            (assoc :STARTDT (parse-date startdt))
            enddt
            (assoc :ENDDT (parse-date enddt)))))

(defn stream-gtin
  [root ch _ close?]
  (let [gtins (zx/xml-> (clojure.zip/xml-zip root) :GTIN_DETAILS :AMPPS :AMPP parse-gtin)]
    (a/<!! (a/onto-chan!! ch gtins false))
    (when close? (a/close! ch))))

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
   :LOOKUP     stream-lookup-xml
   :GTIN       stream-gtin
   :BNF        stream-nested-dmd})

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
  - close?   : close the channel when done?, default true
  - include  : a set of dm+d file types to include (e.g. #{:VTM})
  - exclude  : a set of dm+d file types to exclude (e.g. #{:VTM})"
  [dir ch & {:keys [_include _exclude close?] :or {close? true} :as opts}]
  (log/info "Importing from " dir)
  (let [files (dmd-file-seq dir opts)]
    (log/info "files found in directory " dir ":" files)
    (doseq [dmd-file files]
      (stream-dmd-file ch false dmd-file))
    (when close? (a/close! ch))))

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


(defn ^:private cardinalities-for-product [dir product-kind product-identifier]
  (let [ch (a/chan 1 (filter #(not= (:TYPE %) [product-kind product-kind])))]
    (a/thread (stream-dmd dir ch :include #{product-kind}))
    (let [counts (loop [result {}]
                   (let [item (a/<!! ch)]
                     (if-not item
                       result
                       (recur (update-in result [(:TYPE item) (get item product-identifier)] (fnil inc 0))))))]
      (reduce-kv (fn [result k v]
                   (let [max-cardinality (apply max (vals v))]
                     (conj result {:TYPE            k
                                   :MAX-CARDINALITY max-cardinality
                                   :CARDINALITY     (if (> max-cardinality 1) :TO-MANY :TO-ONE)}))) [] counts))))

(defn cardinalities
  "Determines the cardinalities for the different product components."
  [dir]
  (map (fn [[kind id]] (cardinalities-for-product dir kind id))
       [[:VMP :VPID]
        [:AMP :APID]
        [:VMPP :VPPID]
        [:AMPP :APPID]
        [:GTIN :AMPPID]]))

(defn print-cardinalities [{:keys [dir]}]
  (println "Processing " dir)
  (dorun (map clojure.pprint/print-table (cardinalities (str dir)))))

(comment
  (map clojure.pprint/print-table (cardinalities "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001")))

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

  (dmd-file-seq "/Users/mark/Downloads/week272021-r2_3-BNF")
  (get-release-metadata "/Users/mark/Downloads/week272021-r2_3-BNF")
  (get-component "/Users/mark/Downloads/week272021-r2_3-BNF" :BNF :BNF)
  (get-component "/var/folders/w_/s108lpdd1bn84sntjbghwz3w0000gn/T/trud15801406225560397483/week352021-r2_3-GTIN-zip" :GTIN :AMPP)
  (dmd-file-seq "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :include #{:VTM :VMP} :exclude #{:VMP})
  (dmd-file-seq "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :include #{:VTM})
  (dmd-file-seq "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :exclude #{:VTM})
  (get-release-metadata "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001")
  (get-release-metadata "/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001")
  (statistics-dmd "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001")
  (statistics-dmd "/Users/mark/Downloads/week272021-r2_3-BNF")
  (get-component "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :LOOKUP :LEGAL_CATEGORY)
  (get-component "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :VTM :VTM)
  (get-component "/Users/mark/Downloads/nhsbsa_dmd_12.1.0_20201214000001" :VMP :VMP)
  (def ri (get-component "/tmp/trud6772717631287944974" :AMPP :REIMBURSEMENT_INFO))
  (frequencies (map :DISP_FEES ri))
  (take 20 ri)
  (filter #(nil? (:DISP_FEES %)) ri)
  (get-component (io/resource "dmd-2021-08-26") :AMP :AP_INGREDIENT)

  (def rdr (io/reader file))
  (def root (xml/parse rdr :skip-whitespace true))
  (def ch (a/chan))
  (a/thread (stream-nested-dmd root ch :BNF true))
  (dotimes [_ 1000] (a/<!! ch))
  (a/<!! ch)

  (def file "/var/folders/w_/s108lpdd1bn84sntjbghwz3w0000gn/T/trud15801406225560397483/week352021-r2_3-GTIN-zip/f_gtin2_0260821.xml")
  (def rdr (io/reader file))
  (def root (xml/parse rdr :skip-whitespace true))
  (def ch (a/chan))
  (a/thread (stream-gtin root ch nil true))
  (a/<!! ch)
  (map clojure.pprint/print-table (cardinalities "/var/folders/w_/s108lpdd1bn84sntjbghwz3w0000gn/T/trud2465267306253668332/"))
  )
