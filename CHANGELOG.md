# Changelog

## 0.1.2 - 2025-11-10

* renamed app image from `stac-fastapi-pgstac-pair-search` to `stac-fastapi-pgstac-pair-search/pair-search-app`
* rebuild `pgstac` image and publish under the name `stac-fastapi-pgstac-pair-search/pgstac-pair-search` with `pair-search` functions 
* removed old SQL files from repository

## 0.1.0 - 2025-11-03

* rewriting the low-level DB queries

## 0.0.5 - 2025-10-23

* fixed conformance URLs


## 0.0.4 - 2025-10-21

* make sure to convert search limit from environment to int


## 0.0.3 - 2025-10-21

* increased the single collection search limit to 1000; make it configurable via environment variable PAIR_SEARCH_COLLECTION_SEARCH_LIMIT (#10)
* added more tests for collections search parameters and fixed some issues (#11)


## 0.0.2 - 2025-10-16

* report pair-search conformance classes (#8)
* add pagination (#9)


## 0.0.1 - 2025-09-24

* first release
