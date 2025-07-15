# Changelog

## 1.1.1
  * Bump dependency versions for twistlock compliance [#46](https://github.com/singer-io/tap-google-search-console/pull/46)

## 1.1.0
  * Updates to run on python 3.11.7 [#41](https://github.com/singer-io/tap-google-search-console/pull/41)

## 1.0.0
  * Add support for new search types discover, googleNews, news [#39](https://github.com/singer-io/tap-google-search-console/pull/39)
  * General release of the tap

## 0.2.1
  * Handle no data exception for sitemaps stream [#38](https://github.com/singer-io/tap-google-search-console/pull/38)

## 0.2.0
  * Code Refactoring [#32](https://github.com/singer-io/tap-google-search-console/pull/32)
    * Sets default value for `ATTRIBUTION_DAYS` parameter as 4 days
    * Adds type hinting, code comments
    * Adds unit test cases for helper methods
    * Adds missing fields in Catalog file

## 0.1.1
  * Request Timeout Implementation [#27](https://github.com/singer-io/tap-google-search-console/pull/27)

## 0.1.0
  * Add API connection check in Discovery Mode [#17](https://github.com/singer-io/tap-google-search-console/pull/17)
  * Add integration tests [#18](https://github.com/singer-io/tap-google-search-console/pull/18)
  * Fix error response error handling [#19](https://github.com/singer-io/tap-google-search-console/pull/19)
  * Fix bug in error response handling [#21](https://github.com/singer-io/tap-google-search-console/pull/21)

## 0.0.11
  * Fix state issue and handle "Quota Exceeded" response [#14](https://github.com/singer-io/tap-google-search-console/pull/14)

## 0.0.10
  * Use singer library function to ensure automatic fields are treated as selected [#12](https://github.com/singer-io/tap-google-search-console/pull/12)

## 0.0.9
  * Indent stream-specific logging to be within the stream's execution block

## 0.0.8
  * Raise error and fail fast if missing pk in record instead of just logging [#9](https://github.com/singer-io/tap-google-search-console/pull/9)

## 0.0.7
  * Add date window looping logic to the `sync` function in `sync.py`. Increase the `row_limit` for `performance_reports` to 10,000.

## 0.0.6
  * Fix/simplify bookmarking and paging issues for organizations with a large number of results. Add 14 day attribution window to account for results lag time.

## 0.0.5
  * Increase `performance_report` endpoints `multipleOf` to 25 decimal digits to accommodate numbers returned from API.

## 0.0.4
  * Fix issue with Sitemaps API; ignore domain property sites for `sitemaps` endpoint.

## 0.0.3
  * Change data types for impressions and clicks in performance reports from number to integer.

## 0.0.2
  * Add performance_report endpoints for summaries by each dimension: date, country, device, page, query.

## 0.0.1
  * Initial commit
