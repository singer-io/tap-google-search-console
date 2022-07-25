from datetime import datetime as dt
from datetime import timedelta
from tap_tester import connections, menagerie, runner
from base import GoogleSearchConsoleBaseTest
from tap_tester.logger import LOGGER


class GoogleSearchConsoleInterruptedSyncTest(GoogleSearchConsoleBaseTest):

    def name(self):
        return "tap_tester_google_search_console_interrupted_sync_test"
    
    @staticmethod
    def get_second_sync_bookmark_date(self):
        return dt.strftime(dt.utcnow()-timedelta(days=2), self.START_DATE_FORMAT)

    def test_run(self):
        """
        - Verify an interrupted sync can resume based on the currently_syncing and stream level bookmark value.
        - Verify only records with replication-key values greater than or equal to the stream level bookmark 
          are replicated on the resuming sync for the interrupted stream.
        - Verify the yet-to-be-synced streams are replicated following the interrupted stream in the resuming sync. 
          (All yet-to-be-synced streams must replicate before streams that were already synced)
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
        
        self.perform_and_verify_table_and_field_selection(
            conn_id, catalog_entries)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################
        
        LOGGER.info("Current Bookmark: {}".format(first_sync_bookmarks))
        interrupted_sync_states = { 'currently_syncing': 'performance_report_date',
                                    'bookmarks': {'performance_report_custom': first_sync_bookmarks.get('bookmarks', {}).get('performance_report_custom', {}),
                                    'performance_report_query': first_sync_bookmarks.get('bookmarks', {}).get('performance_report_query', {}),
                                    'performance_report_date': {list(expected_replication_keys['messages'])[0]: self.get_second_sync_bookmark_date()},
                                    'performance_report_country': {list(expected_replication_keys['activity_logs'])[0]: self.start_date},
                                    'performance_report_page': {list(expected_replication_keys['activity_logs'])[0]: self.start_date},
                                    'performance_report_device': {list(expected_replication_keys['activity_logs'])[0]: self.start_date}}}        
        completed_streams = ['performance_report_custom', 'performance_report_query']
        pending_streams = ['performance_report_country', 'performance_report_page', 'performance_report_device']

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

                # expected values
                expected_replication_method = expected_replication_methods[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                # first_sync_messages = [record.get('data') for record in
                #                        first_sync_records.get(
                #                            stream, {}).get('messages', [])
                #                        if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(
                                            stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']
                first_bookmark_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)

                if expected_replication_method == self.INCREMENTAL:

                    # collect information specific to incremental streams from syncs 1 & 2
                    replication_key = next(iter(expected_replication_keys[stream]))
                    
                    interrupted_bookmark_value = interrupted_sync_states['bookmarks'][stream]

                    if stream in completed_streams:
                        # Verify at least 1 record was replicated in the third sync
                        if second_bookmark_value[replication_key] == first_bookmark_value[replication_key]:
                            self.assertEquals(second_sync_count,
                                            1, 
                                            msg="Incorrent bookmarking for {}, at least one record should be replicated".format(stream))
                        else:
                            self.assertGreater(second_sync_count,
                                                1,
                                                msg="Incorrent bookmarking for {}, more than one records should be replicated if second sync bookmark is greater than first sync".format(stream))

                    elif stream == interrupted_sync_states.get('currently_syncing', None):
                        # For interrupted stream records sync count should be less equals
                        self.assertLessEqual(second_sync_count,
                                            first_sync_count,
                                            msg="For interrupted stream, seconds sync record count should be lesser or equal to first sync".format(stream))

                    elif stream in pending_streams:
                        # First sync and second sync record count match
                        if second_bookmark_value[replication_key] == first_bookmark_value[replication_key]:
                            self.assertEquals(second_sync_count,
                                            first_sync_count,
                                            msg="For pending sync stream, if bookmark values are same for first and second sync, record should match".format(stream))
                        else:
                            self.assertGreaterEqual(second_sync_count,
                                                    first_sync_count,
                                                    msg="For pending sync streams, second sync record count should be more than first sync".format(stream))     

                    else:
                        raise Exception("Invalid state of stream {0} in interrupted state, please update appropriate state for the stream".format(stream))
                    
                    for record in second_sync_messages:
                        # Verify the second sync replication key value is Greater or Equal to the first sync bookmark
                        replication_key_value = record.get(replication_key)

                        self.assertLessEqual(interrupted_bookmark_value[replication_key],
                                            replication_key_value,
                                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(replication_key_value,
                                            second_bookmark_value[replication_key],
                                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")
