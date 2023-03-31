from datetime import datetime as dt
from datetime import timedelta

from base import GoogleSearchConsoleBaseTest
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER


class GoogleSearchConsoleBookMarkTest(GoogleSearchConsoleBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_google_search_console_bookmark_test"

    def get_second_sync_bookmark_date(self):
        return dt.strftime(dt.utcnow() - timedelta(days=7), "%Y-%m-%dT00:00:00.000000Z")

    def test_run(self):
        """Verify that for each stream you can do a sync which records
        bookmarks.
        That the bookmark is the maximum value sent to the target for
        the replication key. That a second sync respects the bookmark
        All data of the second sync is >= the bookmark from the first
        sync The number of records in the 2nd sync is less than the
        first (This assumes that new data added to the stream is
        done at a rate slow enough that you haven't doubled the
        amount of data from the start date to the first sync between the
        first sync and second sync run in this test) Verify that for
        full table stream, all data replicated in sync 1 is replicated
        again in sync 2.
        PREREQUISITE For EACH stream that is incrementally replicated
        there are multiple rows of data with
        different values for the replication key
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
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################

        LOGGER.info(f"Current Bookmark: {first_sync_bookmarks}")
        second_sync_bookmark_date = self.get_second_sync_bookmark_date()
        new_states = {
            "currently_syncing": None,
            "bookmarks": {
                "performance_report_device": {
                    site_url: {
                        "image": second_sync_bookmark_date,
                        "web": second_sync_bookmark_date,
                        "video": second_sync_bookmark_date,
                        "news": second_sync_bookmark_date,
                        "googleNews": second_sync_bookmark_date,
                    }
                },
                "performance_report_page": {
                    site_url: {
                        "image": second_sync_bookmark_date,
                        "web": second_sync_bookmark_date,
                        "video": second_sync_bookmark_date,
                        "discover": second_sync_bookmark_date,
                        "news": second_sync_bookmark_date,
                        "googleNews": second_sync_bookmark_date,
                    }
                },
                "performance_report_country": {
                    site_url: {
                        "image": second_sync_bookmark_date,
                        "web": second_sync_bookmark_date,
                        "video": second_sync_bookmark_date,
                        "discover": second_sync_bookmark_date,
                        "news": second_sync_bookmark_date,
                        "googleNews": second_sync_bookmark_date,
                    }
                },
                "performance_report_query": {
                    site_url: {
                        "image": second_sync_bookmark_date,
                        "web": second_sync_bookmark_date,
                        "video": second_sync_bookmark_date,
                        "news": second_sync_bookmark_date,
                    }
                },
                "performance_report_date": {
                    site_url: {
                        "image": second_sync_bookmark_date,
                        "web": second_sync_bookmark_date,
                        "video": second_sync_bookmark_date,
                        "discover": second_sync_bookmark_date,
                        "news": second_sync_bookmark_date,
                        "googleNews": second_sync_bookmark_date,
                    }
                },
                "performance_report_custom": {
                    site_url: {
                        "image": second_sync_bookmark_date,
                        "web": second_sync_bookmark_date,
                        "video": second_sync_bookmark_date,
                        "discover": second_sync_bookmark_date,
                        "news": second_sync_bookmark_date,
                        "googleNews": second_sync_bookmark_date,
                    }
                },
            },
        }

        menagerie.set_state(conn_id, new_states)
        LOGGER.info(f"New Bookmark: {menagerie.get_state(conn_id)}")

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

                # Collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [
                    record.get("data")
                    for record in first_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]

                second_sync_messages = [
                    record.get("data")
                    for record in second_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]

                first_bookmark_value = first_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)
                second_bookmark_value = second_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)

                if expected_replication_method == self.INCREMENTAL:

                    # Collect information specific to incremental streams from syncs 1 & 2
                    # Set date as replication key for performance_report_page since only date is being used for filtering
                    replication_key = (
                        next(iter(expected_replication_keys[stream])) if stream != "performance_report_page" else "date"
                    )

                    simulated_bookmark_value = new_states["bookmarks"][stream]

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_value)

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_value)

                    # Verify second sync record count is less than or equal to first sync record
                    self.assertLessEqual(second_sync_count, first_sync_count)

                    # Verify the second sync bookmark is Greater or Equal to the first sync bookmark for search types
                    self.assertGreaterEqual(
                        second_bookmark_value.get(site_url).get("web"), first_bookmark_value.get(site_url).get("web")
                    )
                    self.assertGreaterEqual(
                        second_bookmark_value.get(site_url).get("image"),
                        first_bookmark_value.get(site_url).get("image"),
                    )
                    self.assertGreaterEqual(
                        second_bookmark_value.get(site_url).get("video"),
                        first_bookmark_value.get(site_url).get("video"),
                    )
                    for record in first_sync_messages:
                        search_type = record.get("search_type", "web")
                        # Verify the first sync bookmark value is the max replication key value for a given stream
                        replication_key_value = record.get(replication_key)
                        self.assertLessEqual(
                            replication_key_value,
                            first_bookmark_value.get(site_url).get(search_type),
                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced.",
                        )

                    for record in second_sync_messages:
                        # Verify the second sync replication key value is Greater or Equal to the first sync bookmark
                        replication_key_value = record.get(replication_key)
                        search_type = record.get("search_type", "web")
                        self.assertLessEqual(
                            simulated_bookmark_value.get(site_url).get(search_type),
                            replication_key_value,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.",
                        )

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(
                            replication_key_value,
                            second_bookmark_value.get(site_url).get(search_type),
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.",
                        )

                    # Verify at least 3 record was replicated in the second sync.
                    # Asserting with 3 since each stream would have data for image, web, video search types
                    self.assertGreaterEqual(
                        second_sync_count, 3, msg=f"We are not fully testing bookmarking for {stream}"
                    )

                else:
                    # Verify bookmark values are none for Full Table streams
                    self.assertIsNone(first_bookmark_value)
                    self.assertIsNone(second_bookmark_value)

                    # Verify record count is same in first and second sync for Full Table streams
                    self.assertEqual(first_sync_record_count[stream], second_sync_record_count[stream])
