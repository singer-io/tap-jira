"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie
from tap_tester.scenario import SCENARIOS
from base import BaseTapTest

class MinimumSelectionTest(BaseTapTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    def name(self):
        return "tap_tester_tap_jira_no_fields_test"

    def do_test(self, conn_id):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        # Select all streams and no fields within streams
        # IF THERE ARE NO AUTOMATIC FIELDS FOR A STREAM
        # WE WILL NEED TO UPDATE THE BELOW TO SELECT ONE
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.select_all_streams_and_fields(conn_id, found_catalogs, select_all_fields=False)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()

        for stream in self.expected_streams():
            with self.subTest(stream=stream):

                # verify that you get more than a page of data
                # SKIP THIS ASSERTION FOR STREAMS WHERE YOU CANNOT GET
                # MORE THAN 1 PAGE OF DATA IN THE TEST ACCOUNT
                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    self.expected_metadata().get(stream, {}).get(self.API_LIMIT, 0),
                    msg="The number of records is not over the stream max limit")

                # verify that only the automatic fields are sent to the target
                expected_fields_for_stream = (self.expected_primary_keys().get(stream, set()) |
                                              self.top_level_replication_key_fields().get(stream, set()) |
                                              self.expected_foreign_keys().get(stream, set()))
                self.assertEqual(
                    actual_fields_by_stream.get(stream, set()),
                    expected_fields_for_stream,
                    msg="The fields sent to the target are not the automatic fields.\nExpected: {}\nActual: {}".format(expected_fields_for_stream, actual_fields_by_stream.get(stream, set())))


SCENARIOS.add(MinimumSelectionTest)
