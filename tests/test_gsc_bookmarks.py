from datetime import timedelta
from datetime import datetime as dt
from tap_tester import connections, menagerie, runner
from base import GoogleSearchConsoleBaseTest
from tap_tester.logger import LOGGER

class GoogleSearchConsoleBookMarkTest(GoogleSearchConsoleBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""

    def name(self):
        return "tap_tester_google_search_console_bookmark_test"
    
    @staticmethod
    def get_second_sync_bookmark_date(self):
        return dt.strftime(dt.utcnow()-timedelta(days=7), self.START_DATE_FORMAT)

    def test_run(self):
        """
        Verify that for each stream you can do a sync which records bookmarks.
        That the bookmark is the maximum value sent to the target for the replication key.
        That a second sync respects the bookmark
            All data of the second sync is >= the bookmark from the first sync
            The number of records in the 2nd sync is less then the first (This assumes that
                new data added to the stream is done at a rate slow enough that you haven't
                doubled the amount of data from the start date to the first sync between
                the first sync and second sync run in this test)
        Verify that for full table stream, all data replicated in sync 1 is replicated again in sync 2.
        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """
        
        
        expected_streams = self.expected_streams()
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        ##########################################################################
        # First Sync
        ##########################################################################
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                           if catalog.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################
        
        LOGGER.info("Current Bookmark: {}".format(first_sync_bookmarks))
        second_sync_bookmark_date = self.get_second_sync_bookmark_date()
        new_states = {'currently_syncing': None,
                      "bookmarks": {"performance_report_device": {"https://www.stitchdata.com/": {"image": second_sync_bookmark_date, "web": second_sync_bookmark_date, "video": second_sync_bookmark_date}},
                                     "performance_report_page": {"https://www.stitchdata.com/": {"image": second_sync_bookmark_date, "web": second_sync_bookmark_date, "video": second_sync_bookmark_date}}, 
                                     "performance_report_country": {"https://www.stitchdata.com/": {"image": second_sync_bookmark_date, "web": second_sync_bookmark_date, "video": second_sync_bookmark_date}},
                                     "performance_report_query": {"https://www.stitchdata.com/": {"image": second_sync_bookmark_date, "web": second_sync_bookmark_date, "video": second_sync_bookmark_date}},
                                     "performance_report_date": {"https://www.stitchdata.com/": {"image": second_sync_bookmark_date, "web": second_sync_bookmark_date, "video": second_sync_bookmark_date}},
                                     "performance_report_custom": {"https://www.stitchdata.com/": {"image": second_sync_bookmark_date, "web": second_sync_bookmark_date, "video": second_sync_bookmark_date}}}}

        menagerie.set_state(conn_id, new_states)
        LOGGER.info("New Bookmark: {}".format(menagerie.get_state(conn_id)))

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Third Sync: 
        #   To test that we get at least one record per replication
        #   Assuming that there no are new records between 2nd and 3rd sync there should only one record,
        #   otherwise we should get more than 1 records
        ##########################################################################

        third_sync_record_count = self.run_and_verify_sync(conn_id)
        third_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Test By Stream
        ##########################################################################

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_replication_method = expected_replication_methods[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(
                                           stream, {}).get('messages', [])
                                       if record.get('action') == 'upsert']

                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(
                                            stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']

                first_bookmark_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)

                # collect information for assertions from syncs 1 & 2 base on expected values
                third_sync_count = third_sync_record_count.get(stream, 0)
                third_bookmark_value = third_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)

                if expected_replication_method == self.INCREMENTAL:

                    # collect information specific to incremental streams from syncs 1 & 2
                    replication_key = next(iter(expected_replication_keys[stream]))
                    
                    simulated_bookmark_value = new_states['bookmarks'][stream]
                    
                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_value)

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_value)

                    # Verify second sync record count is less than or equal to first sync record
                    self.assertLessEqual(second_sync_count, first_sync_count)

                    # Verify the second sync bookmark is Greater or Equal to the first sync bookmark
                    self.assertGreaterEqual(second_bookmark_value.get(replication_key), first_bookmark_value.get(replication_key))

                    for record in first_sync_messages:

                        # Verify the first sync bookmark value is the max replication key value for a given stream
                        replication_key_value = record.get(replication_key)
   
                        self.assertLessEqual(replication_key_value,
                                            first_bookmark_value[replication_key],
                                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")
                    
                    for record in second_sync_messages:
                        # Verify the second sync replication key value is Greater or Equal to the first sync bookmark
                        replication_key_value = record.get(replication_key)

                        self.assertLessEqual(simulated_bookmark_value[replication_key],
                                            replication_key_value,
                                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(replication_key_value,
                                            second_bookmark_value[replication_key],
                                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")
                        
                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))

                # Verify at least 1 record was replicated in the third sync
                if second_bookmark_value[replication_key] == third_bookmark_value[replication_key]:
                    self.assertEquals(third_sync_count, 1, msg="We are not fully testing bookmarking for {}".format(stream))
                else:
                    self.assertGreater(third_sync_count, 1, msg="We are not fully testing bookmarking for {}".format(stream))