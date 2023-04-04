from base import GoogleSearchConsoleBaseTest
from tap_tester import connections, runner


class GoogleConsoleAutomaticFields(GoogleSearchConsoleBaseTest):
    """Verify that for each stream you can get multiple pages of data when no
    fields are selected and only the automatic fields are replicated."""

    @staticmethod
    def name():
        return "tap_tester_google_console_search_automatic_fields"

    def test_run(self):
        """• Verify we can deselect all fields except when inclusion=automatic,
        which is handled by base.py methods.
        • Verify that only the automatic fields are sent to the target.
        • Verify that all replicated records have unique primary key
        values.
        """

        expected_streams = self.expected_streams()

        # Instantiate connection
        conn_id = connections.ensure_connection(self)

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and Field selection
        test_catalogs_automatic_fields = [
            catalog for catalog in found_catalogs if catalog.get("stream_name") in expected_streams
        ]

        self.perform_and_verify_table_and_field_selection(
            conn_id,
            test_catalogs_automatic_fields,
            select_all_fields=False,
        )

        # Run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_keys = self.expected_automatic_fields().get(stream)

                # Collect actual values
                data = synced_records.get(stream)
                record_messages_keys = [set(row["data"].keys()) for row in data["messages"]]
                primary_keys_list = [
                    tuple(message.get("data").get(expected_pk) for expected_pk in expected_keys)
                    for message in data.get("messages")
                    if message.get("action") == "upsert"
                ]
                unique_primary_keys_list = set(primary_keys_list)

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    0,
                    msg="The number of records is not over the stream max limit",
                )

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                # Verify if all the replicated records have unique primary key values
                self.assertEqual(
                    len(unique_primary_keys_list),
                    len(primary_keys_list),
                    msg="Replicated record does not have unique primary key values.",
                )
