# CHANGELOG

# v1.0.??? (not yet released)

* Return ATC matching to same approach as before switch to SQLite and support '*' and '?' for wildcards

# v1.0.200 - 2024-04-27

* Automated tests using GitHub Actions on every commit to main branch and weekly with every release of dm+d in the UK
* Added function specifications for main APIs
* Updated dependencies

# v1.0.180 - 2024-03-03

* First release with new SQLite backing store
 
# v1.0.170-alpha - 2024-02-03

* Switch to using SQLite for backing store

# v0.6.141 - 2022-05-11

* New API endpoint to search for a product by exact name
* New build system using tools.build

# v0.5.3 - 2021-10-03

* Include metadata with each database permitting checks for compatibility and versioning
* Upgraded dependencies, including datalevin 0.5.22
* Simplified ECL generation of products, now *only* including VTMs,AMPs and VMPs by design.
* Add fetch of VMPPs and AMPPs for a product to backing store API.
* Permit resolution of ATC codes from any product arbitrarily.

# v0.5.2 - 2021-09-12

* Include option to include product packs [this feature later removed]

# v0.5.1 - 2021-09-12

* Turn ATC codes / regular expressions into SNOMED expressions that can be expanded by a terminology server such as [hermes](https://github.com/wardle/hermes)

# v0.5.0 - 2021-09-12

* ATC mapping to VTM, VMP and AMPs
* Fully denormalise products recursing into to-one and to-many relationships such as lookups

# v0.4.0 - 2021-09-11

* Map between dm+d products (e.g. get all VMPs for that VTM)

# v0.3.0 - 2021-09-07

* Switch to using numeric fields, and therefore results, for lookup codes.

# v0.2.0 - 2021-09-06

* Expand denormalization to include BNF DDD reference for VMPs
* Fix GTIN import 
* Add synthetic BNF data for testing
* Add lookup tests
* Add continuous integration tests
* Significant improvements in functionality
* Switch to new datalog-based backend (using lmdb not mapdb).

# v0.1.0  - 2021-04-14

* Basic dm+d functionality
