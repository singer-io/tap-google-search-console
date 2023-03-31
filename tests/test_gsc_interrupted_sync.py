from datetime import datetime as dt
from datetime import timedelta

from base import GoogleSearchConsoleBaseTest
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER


class GoogleSearchConsoleInterruptedSyncTest(GoogleSearchConsoleBaseTest):
    @staticmethod
    def name():
        return "tap_tester_google_search_console_interrupted_sync"

    def get_second_sync_bookmark_date(self):
        return dt.strftime(dt.utcnow() - timedelta(days=2), "%Y-%m-%dT00:00:00.000000Z")

    def get_start_date(self):
        return dt.strftime(self.start_date, "%Y-%m-%dT00:00:00.000000Z")

    def test_run(self):
        """
        - Verify an interrupted sync can resume based on the currently_syncing and stream level bookmark value.
        - Verify only records with replication-key values greater than or equal to the stream level bookmark
          are replicated on the resuming sync for the interrupted stream.
        - Verify the yet-to-be-synced streams are replicated following the interrupted stream in the resuming sync.
          (All yet-to-be-synced streams must replicate before streams that were already synced)
        """

        # Excluding the custom report stream to decrease the CCI job duration
        # Currently custom report stream takes more than an hour to sync data for past 14 days
        expected_streams = self.expected_streams() - self.exclude_streams()
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()
        site_url = self.get_properties().get("site_urls", []).split(",")[0]

        ##########################################################################
        # First Sync
        ##########################################################################
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        catalog_entries = [catalog for catalog in found_catalogs if catalog.get("tap_stream_id") in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################

        LOGGER.info(f"Current Bookmark: {first_sync_bookmarks}")
        interrupted_sync_states = {
            "currently_syncing": "performance_report_date",
            "bookmarks": {
                "performance_report_query": {
                    site_url: {
                        "web": first_sync_bookmarks.get("bookmarks", {})
                        .get("performance_report_query", {})
                        .get(site_url)
                        .get("web"),
                        "image": first_sync_bookmarks.get("bookmarks", {})
                        .get("performance_report_query", {})
                        .get(site_url)
                        .get("image"),
                        "video": first_sync_bookmarks.get("bookmarks", {})
                        .get("performance_report_query", {})
                        .get(site_url)
                        .get("video"),
                        "news": first_sync_bookmarks.get("bookmarks", {})
                        .get("performance_report_query", {})
                        .get(site_url)
                        .get("news"),
                    }
                },
                "performance_report_date": {
                    site_url: {
                        "web": self.get_second_sync_bookmark_date(),
                        "image": self.get_second_sync_bookmark_date(),
                        "video": self.get_second_sync_bookmark_date(),
                        "news": self.get_start_date(),
                        "discover": self.get_start_date(),
                        "googleNews": self.get_start_date(),
                    }
                },
                "performance_report_country": {
                    site_url: {
                        "web": self.get_start_date(),
                        "image": self.get_start_date(),
                        "video": self.get_start_date(),
                        "news": self.get_start_date(),
                        "discover": self.get_start_date(),
                        "googleNews": self.get_start_date(),
                    }
                },
                "performance_report_page": {
                    site_url: {
                        "web": self.get_start_date(),
                        "image": self.get_start_date(),
                        "video": self.get_start_date(),
                        "news": self.get_start_date(),
                        "discover": self.get_start_date(),
                        "googleNews": self.get_start_date(),
                    }
                },
                "performance_report_device": {
                    site_url: {
                        "web": self.get_start_date(),
                        "image": self.get_start_date(),
                        "video": self.get_start_date(),
                        "news": self.get_start_date(),
                        "googleNews": self.get_start_date(),
                    }
                },
            },
        }
        completed_streams = ["performance_report_query"]
        pending_streams = ["performance_report_country", "performance_report_page", "performance_report_device"]

        menagerie.set_state(conn_id, interrupted_sync_states)

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)
        ##########################################################################
        # Test By Stream
        ##########################################################################

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_replication_method = expected_replication_methods[stream]

                # Collect information for assertions from syncs 1 & 2 based on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                second_sync_messages = [
                    record.get("data")
                    for record in second_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                second_bookmark_value = second_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)

                if expected_replication_method == self.INCREMENTAL:

                    # Collect information specific to incremental streams from syncs 1 & 2
                    # Set date as replication key for performance_report_page since only date is being used for filtering
                    replication_key = (
                        next(iter(expected_replication_keys[stream])) if stream != "performance_report_page" else "date"
                    )

                    interrupted_bookmark_value = interrupted_sync_states["bookmarks"][stream]
                    if stream in completed_streams:
                        # Verify at least 1 record was replicated in the second sync
                        self.assertGreaterEqual(
                            second_sync_count,
                            1,
                            msg=f"Incorrect bookmarking for {stream}, more than one records should be replicated if second sync bookmark is greater than first sync",
                        )

                    elif stream == interrupted_sync_states.get("currently_syncing", None):
                        # For interrupted stream records sync count should be less equals
                        self.assertLessEqual(
                            second_sync_count,
                            first_sync_count,
                            msg=f"For interrupted stream {stream}, seconds sync record count should be lesser or equal to first sync",
                        )

                    elif stream in pending_streams:
                        # First sync and second sync record count match
                        self.assertGreaterEqual(
                            second_sync_count,
                            first_sync_count,
                            msg=f"For pending sync streams {stream}, second sync record count should be more than first sync",
                        )

                    else:
                        raise Exception(
                            f"Invalid state of stream {stream} in interrupted state, please update appropriate state for the stream"
                        )

                    for record in second_sync_messages:
                        # Get the search_type of the record to fetch the appropriate bookmark value from interrupted_bookmarks
                        search_type = record.get("search_type", "web")
                        # Verify the second sync replication key value is Greater or Equal to the first sync bookmark
                        replication_key_value = record.get(replication_key)

                        self.assertLessEqual(
                            interrupted_bookmark_value.get(site_url).get(search_type),
                            replication_key_value,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.",
                        )

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(
                            replication_key_value,
                            second_bookmark_value.get(site_url).get(search_type),
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.",
                        )
