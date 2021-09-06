# dmd

[![Scc Count Badge](https://sloc.xyz/github/wardle/dmd)](https://github.com/wardle/dmd/)
[![Scc Cocomo Badge](https://sloc.xyz/github/wardle/dmd?category=cocomo&avg-wage=100000)](https://github.com/wardle/dmd/)
[![CircleCI](https://circleci.com/gh/wardle/dmd.svg?style=shield)](https://circleci.com/gh/wardle/dmd)


Implementation of the UK NHS dictionary of medicines and devices (dm+d).

This is a lightweight library and microservice for UK dm+d data. It is a thin wrapper around the dm+d data; providing a
fast key value backing store functionality.

The test harness contains an extract of dm+d data that represents public sector information licensed under the 
[Open Government Licence v3.0](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/). These data
are reproduced in order to support automated software code tests. 
The results of these automated tests are shown above.

`dmd` is designed to be composable with other services, including [hermes](https://github.com/wardle/hermes). Fortunately,
dm+d codes are actually SNOMED identifiers, which means that as long as you run a terminology service that includes the
UK drug extension, you will be able to seamlessly process data across both `hermes` and `dmd`. They are naturally
composable and yet can be developed in parallel.

SNOMED releases occur at three-monthly intervals. The NHS Business Services Authority (BSA) release dm+d data weekly.
That means there may be products within dm+d that are not yet within the UK SNOMED CT drug extension. That was one of
the reasons I decided to create `dmd` as a separate service.

# Getting started

`dmd` creates what you should regard as an  immutable file-based database. 
That database can then be used by `dmd` whether operating as a library or a 
microservice.

#### 1. Install clojure, if not already available

You can either install clojure and run from source code, or download a runnable jar file. 

I suggest [installing clojure](https://clojure.org/guides/getting_started) for the easiest experience.
If you prefer, download a runnable jar file from the [published releases](https://github.com/wardle/dmd/releases).

#### 2. Register for a TRUD account and (optionally) download an api key

You will need to get a dm+d distribution via [TRUD](https://isd.digital.nhs.uk), which requires registration. 
The dm+d data is published by the NHS Business Services Authority under an Open Government Licence, but you will need
an NHS digital TRUD account in order to download a distribution.

If you want to use automatic downloads, you will also need to download an API key
from the service.  Copy and paste that key into a file on your local filesystem, if required.

e.g.
```shell
echo >/var/local/trud/api-key.txt XXXXXXXXX
```

#### 3. Download and install the latest dm+d distribution

You can choose to download manually, or automatically. I'd recommend automatic downloading.

##### Download manually

You can download a dm+d distribution from the NHS Digital's [TRUD]((https://isd.digital.nhs.uk)) service, on behalf of the NHS BSA. 
Download the main dm+d distribution; you can optionally also download the 'extras' which include mapping from dm+d to 
ATC BNF codes. Once downloaded, unzip one or both into a common directory and run: 

```shell
clj -M:run install --db dmd-2021-07-01.db ~/Downloads/dmd-2021-07-01 
```
or
```shell
java -jar dmd.jar --db dmd-2021-07-01.db ~/Downloads/dmd-2021-07-01 
```

This will look for files in the directory specified and create a new file-based
database at `dmd-2021-07-01.db`. 
You can also specify multiple directories from which to import.

##### Download automatically

`dmd` has automatic functionality to download and install the latest distribution. The process takes a few
minutes.

Automatic downloads need a cache directory. This stops automated tools like this from overloading NHS Digital's
infrastructure. If the right file with the right checksum is already available in your local cache, download will be
avoided.

Here I use `/var/local/trud/cache' as my cache directory. I share this among a number of tools.

```shell
clj -M:run latest --api-key /var/local/trud/api-key.txt --cache-dir /var/local/trud/cache
```
or
```shell
java -jar dmd.jar latest --api-key /var/local/trud/api-key.txt --cache-dir /var/local/trud/cache
```

This will create a file-based database named dmd-2021-04-12.db (or whatever release date is appropriate).
Alternatively, you can manually specify a name:

```shell
clj -M:run latest --api-key /var/local/trud/api-key.txt --cache-dir /var/local/trud/cache --db dmd.db
```
or
```shell
java -jar dmd.jar latest --api-key /var/local/trud/api-key.txt --cache-dir /var/local/trud/cache --db dmd.db
```

Note: this does not support updating an existing datafile. It is much better
to build a new datafile for each release of the dm+d distribution. You should
regard that file as immutable after creation. This is reference data; do not
update in place. 

You can find available releases on TRUD by looking at their [portal](https://isd.digital.nhs.uk)
or running:

```shell
clj -M:run list --api-key /var/local/trud/api-key.txt
```
or
```shell
java -jar dmd.jar list --api-key /var/local/trud/api-key.txt
```


Result:
```shell
➜  dmd git:(main) ✗  clj -M:run list --api-key /var/local/trud/api-key.txt

|                                  :id | :releaseDate |          :name |
|--------------------------------------+--------------+----------------|
|  nhsbsa_dmd_4.1.0_20210412000001.zip |   2021-04-12 |  Release 4.1.0 |
|  nhsbsa_dmd_4.0.0_20210405000001.zip |   2021-04-05 |  Release 4.0.0 |
|  nhsbsa_dmd_3.4.0_20210329000001.zip |   2021-03-29 |  Release 3.4.0 |
|  nhsbsa_dmd_3.3.0_20210322000001.zip |   2021-03-22 |  Release 3.3.0 |
```

To download a specific edition, you can download manually, unzip and then 
directly import from that distribution.

```shell
clj -M:run install --db dmd.db  /tmp/downloads/trud
```

Or you can specify the version and let `dmd` download for you.
Specify which release you want by date according to ISO-8601 standard:

```shell
clj -M:run --api-key /var/local/trud/api-key.txt download 2021-04-05
```

#### 4. Run a HTTP server  

Once you have downloaded a distribution, you can use it to run a very fast HTTP server.

```shell
clj -M:run serve --db dmd-2021-04-12.db --port 8080
```
or
```shell
java -jar dmd.jar --db dmd-2021-04-12.db --port 8080
```

As it is very likely that the complete dm+d dataset will fit into the memory
of a small server, this service can scale easily. It is entirely reasonable
to run behind an API gateway and share a read-only filesystem across multiple
instances should it be necessary. It is also entirely reasonable to keep
older instances running to provide access to data from a particular time point
should that be necessary. Clients can then choose to use a specific version
or simply the latest dataset.  

The HTTP server is under active development. There are a number of endpoints available that simplify obtaining and
making sense of dm+d data.

These endpoints are complete:

* Get denormalized information for a given product. 
* Map between BNF or ATC codes and dm+d products. 

These endpoints are currently under development and will be released shortly:

* Get associated products for any given product.
* Convert between product-based prescribing models to dose-based models and back.

Further documentation will be forthcoming.

##### Get denormalized information about a product.

Example usage:
```shell
$ http -j http://localhost:8080/dmd/v1/product/12797611000001109 
```

Result will be the denormalized product. Each product will include
important references such as ingredients, excipients and the associated
products. A VMP will include its VTM. An AMP or VMPP will include its VMP, and
the VTM, and an AMPP will include all of the others!

```json
{
  "BASIS": {
    "CD": "0001",
    "DESC": "rINN - Recommended International Non-proprietary"
  },
  "BASISCD": "0001",
  "BASIS_PREVCD": "0001",
  "BNF_DETAILS": {
    "ATC": "C08CA01",
    "DDD": 5.0,
    "DDD_UOM": {
      "CD": 258684004,
      "DESC": "mg"
    },
    "DDD_UOMCD": 258684004,
    "VPID": 12797611000001109
  },
  "CONTROL_DRUG_INFO": {
    "CD": "0000",
    "DESC": "No Controlled Drug Status"
  },
  "DF_IND": {
    "CD": "2",
    "DESC": "Continuous"
  },
  "DF_INDCD": "2",
  "DRUG_FORM": {
    "CD": 385023001,
    "DESC": "Oral solution"
  },
  "DRUG_ROUTES": [
    {
      "CD": 26643006,
      "DESC": "Oral"
    }
  ],
  "ID": 12797611000001109,
  "INGREDIENTS": [
    {
      "BASIS_STRNT": {
        "CD": "0001",
        "DESC": "Based on Ingredient Substance"
      },
      "BASIS_STRNTCD": "0001",
      "IS": {
        "ISID": 386864001,
        "ISIDDT": "2005-07-27",
        "ISIDPREV": 3512211000001104,
        "NM": "Amlodipine"
      },
      "ISID": 386864001,
      "STRNT_DNMTR_UOMCD": 258773002,
      "STRNT_DNMTR_VAL": 1.0,
      "STRNT_NMRTR_UOMCD": 258684004,
      "STRNT_NMRTR_VAL": 1.0,
      "VPID": 12797611000001109
    }
  ],
  "INVALID": true,
  "NM": "Amlodipine 5mg/5ml oral solution sugar-free",
  "NMDT": "2015-06-08",
  "NMPREV": "Amlodipine 5mg/5ml oral solution sugar free",
  "NON_AVAIL": {
    "CD": "0001",
    "DESC": "Actual Products not Available"
  },
  "NON_AVAILCD": "0001",
  "NON_AVAILDT": "2016-12-06",
  "ONT_DRUG_FORMS": [
    {
      "CD": "0005",
      "DESC": "solution.oral"
    }
  ],
  "PRES_STAT": {
    "CD": "0001",
    "DESC": "Valid as a prescribable product"
  },
  "PRES_STATCD": "0001",
  "SUG_F": true,
  "TYPE": "VMP",
  "VPID": 12797611000001109,
  "VTM": {
    "ID": 108537001,
    "NM": "Amlodipine",
    "TYPE": "VTM",
    "VTMID": 108537001
  },
  "VTMID": 108537001
}

```

##### Map from BNF or ATC codes to VMPs

Here I look for ATC codes beginning with L04AX.
Anatomical Therapeutic Chemical (ATC) codes are a useful way of categorising drugs,
and so you can use `dmd` to generate codelists for operational, realtime or 
offline analytics.

You can use a regular expression here - so L04AX.* asks for ATC codes that start with L04AX

```shell
http -j localhost:8080/dmd/v1/atc/L04AX.*/vmps
```

Result:
```json
[
    {
        "NM": "Thalidomide 100mg tablets",
        "VPID": 10194111000001109
    },
    {
        "NM": "Azathioprine 30mg/5ml oral suspension",
        "VPID": 8308811000001100
    },
    {
        "NM": "Azathioprine 15mg/5ml oral suspension",
        "VPID": 8283211000001104
    },
    {
        "NM": "Methotrexate 750micrograms/5ml oral suspension",
        "VPID": 12815511000001103
    },
    {
        "NM": "Azathioprine 30mg capsules",
        "VPID": 8791411000001107
    },
  ...
```

#### 5. Embed as a library into a larger application

While `dmd` can be run as an independent microservice, it can also be embedded into a larger application or service as a
library.

eg. use a git coordinate in deps.edn:

```clojure
com.eldrix/dmd {:git/url "https://github.com/wardle/dmd.git"
                :sha     "XXX"}
```

# Frequently asked questions

### What is the use of `dmd`?

dmd provides a simple library, and optionally a microservice, to help you make use of the UK dm+d.

A library can be embedded into your application; this is easy using Clojure or Java. You make calls using the dmd API
just as you'd use any regular library.

A microservice runs independently and you make use of the data and software by making an API call over the network.

Like all `PatientCare` components, you can use `dmd` in either way. Usually, when you're starting out, it's best to use
as a library but larger projects and larger installations will want to run their software components independently,
optimising for usage patterns, resilience, reliability and rate of change.

The current focus is on making dm+d data available in your applications.

The next focus will be on providing functionality to support more sophisticated
processing of prescribing data, in particular a way to round-trip between
dose-based and product-based prescribing. Actually, all of this code has been
designed to support building that functionality!

### Why do I need separate tooling for dm+d?

Previously, I implemented dm+d within an EPR and later within a SNOMED terminology server. However, the UK drug
extension lacks some of the data that is included in the NHS BSA dm+d distribution, such as the concrete data that are
needed in order to safely map from dose-based prescribing to product-based prescribing. It is difficult to build
software that processes prescribing data without those concrete values documenting how much of each ingredient is in
each product.

`dmd` provides data and computing services for the full UK `dm+d` dataset. It's there if you need to supplement the UK
SNOMED drug extension with additional data. That will almost certainly be the case if you're writing an e-prescribing
system.

If the SNOMED-based UK drug extension provides all of the information you need in your user-facing applications, you
won't need to use `dm+d`; simply use [hermes](https://github.com/wardle/hermes) instead.

### Why are you building so many small repositories?

Small modules of functionality are easier to develop, easier to understand, easier to test and easier to maintain. I
design modules to be composable so that I can stitch different components together in order to solve problems.

In larger systems, it is easy to see code rotting. Dependencies become outdated and the software becomes difficult to
change easily because of software that depend on it. Small, well-defined modules are much easier to build and are less
likely to need ongoing changes over time; my goal is to need to update modules only in response to changes in *domain*
not software itself. Accretion of functionality.

It is very difficult to 'prove' software is working as designed when there are lots of moving parts.

One of the core abtractions used across the PatientCare components is identifier resolution and mapping. It might be
possible to use a single terminology server to import a range of what HL7 FHIR regards as a terminology, or a value set,
but that couldn't work for me.

With a federatable model, I can easily add new terminologies and value sets, such as resolving dm+d identifiers or OMIM
references, to a loosely-coupled terminology server. 

I am building multiple discrete but composable data and computing services - they are building blocks.

### Did you consider alternative approaches?

Yes. It is better to start with data and build computing services around those
data and map to higher-level abstractions at run-time. I used to import and map
at the time of import, but that presupposes that I know what I will need both
now and in the future, and experience has taught me that I know neither!

### Why are you creating building blocks and not user-facing applications?

I am building user-facing applications by composing these building blocks together. 
The shiny applications are under development, but we need to start with the
basics.

I have spent a lot of time trying to influence how we build software within the public sector,
and while I've made progress, most decision-makers are neither technical nor clinical. 

Instead, I think the best way to effect change is to demonstrate how we 
can build health and care software in a new way, focusing on data-centricity, openness and 
adopting a standards-based approach. Open-source is a critical component of that.

There is too much incidental complexity in health and care software, and too many moving parts. 
The domains of health and care are too complex to add to that burden by using the wrong
technical and architectural approaches. These tools show a new approach based on data
and its flow between loosely-coupled components. 

### Why do you put COCOMO estimates on your repositories?

The COCOMO model provides an estimate of the cost of software.
I don't think it is accurate, at all, but when you look at open-source software
it is important to recognise that open-source work is not free. There is a cost
and it is sensible to remind people of that.

### What is the structure of dm+d?

There is good information available online. 
The latest documentation is from 2017 from the NHSBSA - 
[https://www.nhsbsa.nhs.uk/sites/default/files/2017-02/Technical_Specification_of_data_files_R2_v3.1_May_2015.pdf](https://www.nhsbsa.nhs.uk/sites/default/files/2017-02/Technical_Specification_of_data_files_R2_v3.1_May_2015.pdf).

There are five main dm+d classes:

* VTM
* VMP
* AMP
* VMPP
* AMPP

Each class has associated properties.
The cardinalities of the different classes and properties are shown on page 5 of that document.

In order to be safe, I have automated reports that process dm+d data to confirm assumptions. 
For example, here is an automated report on dm+d data confirming the cardinalities:

```
|                              :TYPE | :MAX-CARDINALITY | :CARDINALITY |
|------------------------------------+------------------+--------------|
| [:VMP :VIRTUAL_PRODUCT_INGREDIENT] |               29 |     :TO-MANY |
|              [:VMP :ONT_DRUG_FORM] |                9 |     :TO-MANY |
|                  [:VMP :DRUG_FORM] |                1 |      :TO-ONE |
|                 [:VMP :DRUG_ROUTE] |                7 |     :TO-MANY |
|          [:VMP :CONTROL_DRUG_INFO] |                1 |      :TO-ONE |

|                  :TYPE | :MAX-CARDINALITY | :CARDINALITY |
|------------------------+------------------+--------------|
|  [:AMP :AP_INGREDIENT] |               16 |     :TO-MANY |
| [:AMP :LICENSED_ROUTE] |                6 |     :TO-MANY |
| [:AMP :AP_INFORMATION] |                1 |      :TO-ONE |

|                     :TYPE | :MAX-CARDINALITY | :CARDINALITY |
|---------------------------+------------------+--------------|
| [:VMPP :DRUG_TARIFF_INFO] |                1 |      :TO-ONE |
|     [:VMPP :COMB_CONTENT] |              875 |     :TO-MANY |

|                               :TYPE | :MAX-CARDINALITY | :CARDINALITY |
|-------------------------------------+------------------+--------------|
|        [:AMPP :APPLIANCE_PACK_INFO] |                1 |      :TO-ONE |
| [:AMPP :DRUG_PRODUCT_PRESCRIB_INFO] |                1 |      :TO-ONE |
|    [:AMPP :MEDICINAL_PRODUCT_PRICE] |                1 |      :TO-ONE |
|         [:AMPP :REIMBURSEMENT_INFO] |                1 |      :TO-ONE |
|               [:AMPP :COMB_CONTENT] |             1085 |     :TO-MANY |

|         :TYPE | :MAX-CARDINALITY | :CARDINALITY |
|---------------+------------------+--------------|
| [:GTIN :AMPP] |                1 |      :TO-ONE |
```

From the documentation, it is not clear that would only be a 1:1 map from AMPP to GTIN, as it looks as if GTINs can
expire. This data quality report confirms the to-one relationship.

It's easy to run this from a clojure REPL in order to test another release, 
or you can run it from the command line:

```shell
clj -X:cardinalities :dir '"/Users/mark/Downloads/nhsbsa_dmd_3.4.0_20210329000001/"'
```



# Developer information

#### View all dependencies

```shell
clj -X:deps tree
```

#### View outdated dependencies

```shell
clj -M:outdated
```

#### Run compilation checks

```shell
clj -M:check
```

#### Run unit tests

```shell
clj -X:test
```

#### Run linting

```shell
clj -M:lint/eastwood
clj -M:lint/kondo
```

#### Build library jar

```shell
clj -X:jar
```

#### Build and run an uberjar

```shell
clj -X:uberjar
java -jar target/dmd.jar --help
```
