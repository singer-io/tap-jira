"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie
from base import BaseTapTest

class MinimumSelectionTest(BaseTapTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    def name(self):
        return "tt_jira_no_fields_test"

    def test_run(self):
        """
        - Verify that for each stream you can get multiple pages of data
        - when no fields are selected and only the automatic fields are replicated.
        - Verify that all replicated records have unique primary key values

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        conn_id = self.create_connection_with_initial_discovery()

        self.create_test_data()

        # Select all streams and no fields within streams
        # IF THERE ARE NO AUTOMATIC FIELDS FOR A STREAM
        # WE WILL NEED TO UPDATE THE BELOW TO SELECT ONE
        found_catalogs = menagerie.get_catalogs(conn_id)

        expected_streams = self.expected_streams()
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in expected_streams]

        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=False)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # get primary keys
                expected_primary_keys = self.expected_primary_keys().get(stream, set())
                # collect records
                messages = synced_records.get(stream)

                # verify that you get more than a page of data
                # SKIP THIS ASSERTION FOR STREAMS WHERE YOU CANNOT GET
                # MORE THAN 1 PAGE OF DATA IN THE TEST ACCOUNT
                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    self.expected_metadata().get(stream, {}).get(self.API_LIMIT),
                    msg="The number of records is not over the stream max limit")

                # verify that only the automatic fields are sent to the target
                expected_fields_for_stream = (expected_primary_keys |
                                              self.top_level_replication_key_fields().get(stream, set()) |
                                              self.expected_foreign_keys().get(stream, set()))
                self.assertEqual(
                    actual_fields_by_stream.get(stream, set()),
                    expected_fields_for_stream,
                    msg="The fields sent to the target are not the automatic fields.\nExpected: {}\nActual: {}".format(expected_fields_for_stream, actual_fields_by_stream.get(stream, set())))

                # Verify that all replicated records have unique primary key values
                records_pks_list = [tuple([message.get('data').get(primary_key) for primary_key in expected_primary_keys])
                                           for message in messages.get('messages')]
                self.assertCountEqual(set(records_pks_list), records_pks_list,
                                      msg="We have duplicate records for {}".format(stream))
