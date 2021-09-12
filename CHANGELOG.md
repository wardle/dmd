# CHANGELOG

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

* Basis dm+d functionality