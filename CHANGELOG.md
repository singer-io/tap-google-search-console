# Changelog

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
