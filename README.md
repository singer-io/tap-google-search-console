# tap-google-search-console

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Google Search Console API](https://developers.google.com/webmaster-tools/search-console-api-original/v3/how-tos/search_analytics)
- Extracts the following resources:
  - [Sites](https://developers.google.com/webmaster-tools/search-console-api-original/v3/sites/get)
  - [Sitemaps](https://developers.google.com/webmaster-tools/search-console-api-original/v3/sitemaps/list)
  - [Performance Reports](https://developers.google.com/webmaster-tools/search-console-api-original/v3/searchanalytics/query)
    - Custom (Summary for Site and Search Type by any combination of Date + Country, Device, Page, Query)
    - Date (Summary for Site and Search Type by Date)
    - Country (Summary for Site and Search Type by Date and Country)
    - Device (Summary for Site and Search Type by Date and Device)
    - Page (Summary for Site and Search Type by Date and Page)
    - Query (Summary for Site and Search Type by Date and Query)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Streams
[**sites (GET)**](https://developers.google.com/webmaster-tools/search-console-api-original/v3/sites/get)
- Endpoint: https://www.googleapis.com/webmasters/v3/sites/{site_url}
- Primary keys: site_url
- Foreign keys: None
- Replication strategy: Full (all sites in config site_urls)
- Transformations: Fields camelCase to snake_case

[**sitemaps (GET)**](https://developers.google.com/webmaster-tools/search-console-api-original/v3/sitemaps/list)
- Endpoint: https://www.googleapis.com/webmasters/v3/sites/{site_url}/sitemaps
- Primary keys: site_url, path, last_submitted
- Foreign keys: site_url
- Replication strategy: Full (all sitemaps for sites in config site_urls)
- Transformations: Fields camelCase to snake_case, string-integers to integers

[**performance_report_country (POST)**](https://developers.google.com/webmaster-tools/search-console-api-original/v3/searchanalytics/query)
- [Performance Report Description](https://support.google.com/webmasters/answer/7576553?hl=en)
- Endpoint: https://www.googleapis.com/webmasters/v3/sites/{site_url}/searchAnalytics/query
- Primary keys: site_url, search_type, date, country
- Foreign keys: site_url
- Replication strategy: Incremental (query filtered based on date)
  - Filters: site_url, searchType, startDate (bookmark), endDate (current date) 
  - Sort by: date ASC (when date is a dimension, results are sorted by date ascending)
  - Bookmark: date (date-time)
- Transformations: Fields camelCase to snake_case, denest dimensions key/values, remove keys list node

[**performance_report_custom (POST)**](https://developers.google.com/webmaster-tools/search-console-api-original/v3/searchanalytics/query)
- [Performance Report Description](https://support.google.com/webmasters/answer/7576553?hl=en)
- Endpoint: https://www.googleapis.com/webmasters/v3/sites/{site_url}/searchAnalytics/query
- Primary keys: site_url, search_type, date, dimensions_hash_key
  - Dimensions: date (required), country, device, page, query (based on catalog selection)
  - dimensions_hash_key: MD5 hash key of ordered list of selected dimension values
- Foreign keys: site_url
- Replication strategy: Incremental (query filtered based on date)
  - Filters: site_url, searchType, startDate (bookmark), endDate (current date) 
  - Sort by: date ASC (when date is a dimension, results are sorted by date ascending)
  - Bookmark: date (date-time)
- Transformations: Fields camelCase to snake_case, denest dimensions key/values, remove keys list node
 
 [**performance_report_date (POST)**](https://developers.google.com/webmaster-tools/search-console-api-original/v3/searchanalytics/query)
- [Performance Report Description](https://support.google.com/webmasters/answer/7576553?hl=en)
- Endpoint: https://www.googleapis.com/webmasters/v3/sites/{site_url}/searchAnalytics/query
- Primary keys: site_url, search_type, date
- Foreign keys: site_url
- Replication strategy: Incremental (query filtered based on date)
  - Filters: site_url, searchType, startDate (bookmark), endDate (current date) 
  - Sort by: date ASC (when date is a dimension, results are sorted by date ascending)
  - Bookmark: date (date-time)
- Transformations: Fields camelCase to snake_case, denest dimensions key/values, remove keys list node

[**performance_report_device (POST)**](https://developers.google.com/webmaster-tools/search-console-api-original/v3/searchanalytics/query)
- [Performance Report Description](https://support.google.com/webmasters/answer/7576553?hl=en)
- Endpoint: https://www.googleapis.com/webmasters/v3/sites/{site_url}/searchAnalytics/query
- Primary keys: site_url, search_type, date, device
- Foreign keys: site_url
- Replication strategy: Incremental (query filtered based on date)
  - Filters: site_url, searchType, startDate (bookmark), endDate (current date) 
  - Sort by: date ASC (when date is a dimension, results are sorted by date ascending)
  - Bookmark: date (date-time)
- Transformations: Fields camelCase to snake_case, denest dimensions key/values, remove keys list node

[**performance_report_page (POST)**](https://developers.google.com/webmaster-tools/search-console-api-original/v3/searchanalytics/query)
- [Performance Report Description](https://support.google.com/webmasters/answer/7576553?hl=en)
- Endpoint: https://www.googleapis.com/webmasters/v3/sites/{site_url}/searchAnalytics/query
- Primary keys: site_url, search_type, date, page
- Foreign keys: site_url
- Replication strategy: Incremental (query filtered based on date)
  - Filters: site_url, searchType, startDate (bookmark), endDate (current date) 
  - Sort by: date ASC (when date is a dimension, results are sorted by date ascending)
  - Bookmark: date (date-time)
- Transformations: Fields camelCase to snake_case, denest dimensions key/values, remove keys list node

[**performance_report_query (keyword) (POST)**](https://developers.google.com/webmaster-tools/search-console-api-original/v3/searchanalytics/query)
- [Performance Report Description](https://support.google.com/webmasters/answer/7576553?hl=en)
- Endpoint: https://www.googleapis.com/webmasters/v3/sites/{site_url}/searchAnalytics/query
- Primary keys: site_url, search_type, date, query
- Foreign keys: site_url
- Replication strategy: Incremental (query filtered based on date)
  - Filters: site_url, searchType, startDate (bookmark), endDate (current date) 
  - Sort by: date ASC (when date is a dimension, results are sorted by date ascending)
  - Bookmark: date (date-time)
- Transformations: Fields camelCase to snake_case, denest dimensions key/values, remove keys list node


## Authentication
The [**Google Search Console Setup & Authentication**](https://drive.google.com/open?id=1FojlvtLwS0-BzGS37R0jEXtwSHqSiO1Uw-7RKQQO-C4) Google Doc provides instructions show how to configure the Google Search Console for your domain and website URLs, configure Google Cloud to authorize/verify your domain ownership, generate an API key (client_id, client_secret), authenticate and generate a refresh_token, and prepare your tap config.json with the necessary parameters.

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-google-search-console
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install target-json
    > pip install target-stitch
    > pip install singer-tools
    > pip install singer-python
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file. Include the client_id, client_secret, refresh_token, site_urls (website URL properties in a comma delimited list; do not include the domain-level property in the list), start_date (UTC format), user_agent (tap name with the api user email address) and request_timeout (the timeout for the requests. Default: 300).

    ```json
    {
        "client_id": "YOUR_CLIENT_ID",
        "client_secret": "YOUR_CLIENT_SECRET",
        "refresh_token": "YOUR_REFRESH_TOKEN",
        "site_urls": "https://example.com, https://www.example.com, http://example.com, http://www.example.com, sc-domain:example.com",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-google-search-console <api_user_email@example.com>",
        "request_timeout": 300
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.
    Only the `performance_reports` uses a bookmark. The date-time bookmark is stored in a nested structure based on the endpoint, site, and sub_type.

    ```json
    {
      "currently_syncing": "sitemaps",
      "bookmarks": {
        "performance_report_custom": {
          "sc-domain:example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-08T00:00:00.000000Z"
          },
          "https://example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-08T00:00:00.000000Z"
          },
          "http://example.com": {
            "web": "2020-04-13T00:00:00.000000Z"
          }
        },
        "performance_report_date": {
          "http://www.example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-18T00:00:00.000000Z"
          },
          "sc-domain:example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-18T00:00:00.000000Z"
          },
          "https://www.example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-18T00:00:00.000000Z"
          },
          "https://example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-18T00:00:00.000000Z"
          },
          "http://example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-18T00:00:00.000000Z"
          }
        },
        "performance_report_device": {
          "sc-domain:example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-08T00:00:00.000000Z"
          },
          "https://example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-08T00:00:00.000000Z"
          },
          "http://example.com": {
            "web": "2020-04-16T00:00:00.000000Z"
          }
        },
        "performance_report_page": {
          "sc-domain:example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-08T00:00:00.000000Z"
          },
          "https://example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-08T00:00:00.000000Z"
          },
          "http://example.com": {
            "web": "2020-04-16T00:00:00.000000Z"
          }
        },
        "performance_report_query": {
          "sc-domain:example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-08T00:00:00.000000Z"
          },
          "https://example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-08T00:00:00.000000Z"
          },
          "http://example.com": {
            "web": "2020-04-13T00:00:00.000000Z"
          }
        },
        "performance_report_country": {
          "sc-domain:example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-08T00:00:00.000000Z"
          },
          "https://example.com": {
            "web": "2020-04-18T00:00:00.000000Z",
            "image": "2020-04-18T00:00:00.000000Z",
            "video": "2020-04-08T00:00:00.000000Z"
          },
          "http://example.com": {
            "web": "2020-04-16T00:00:00.000000Z"
          }
        }
      }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-google-search-console --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-google-search-console --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-google-search-console --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-google-search-console --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the Google Search Console tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_google_search_console -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.82/10.
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-google-search-console --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid.
    It contained 42192 messages for 8 streams.

          8 schema messages
      42118 record messages
        66 state messages

    Details by stream:
    +----------------------------+---------+---------+
    | stream                     | records | schemas |
    +----------------------------+---------+---------+
    | sites                      | 5       | 1       |
    | sitemaps                   | 4       | 1       |
    | performance_report_date    | 1170    | 1       |
    | performance_report_page    | 3640    | 1       |
    | performance_report_country | 7371    | 1       |
    | performance_report_query   | 11335   | 1       |
    | performance_report_device  | 639     | 1       |
    | performance_report_custom  | 17954   | 1       |
    +----------------------------+---------+---------+
    ```
---

Copyright &copy; 2019 Stitch
