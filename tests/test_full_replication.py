"""
Test tap gets all records for streams with full replication
"""
import json

from tap_tester import menagerie, runner
from tap_tester.logger import LOGGER
from base import BaseTapTest


class FullReplicationTest(BaseTapTest):
    """Test tap gets all records for streams with full replication"""

    @staticmethod
    def name():
        return "tt_name_full_test"

    def test_run(self):
        """
        Verify that a bookmark doesn't exist for the stream
        Verify that the second sync includes the same number or more records than the first sync
        Verify that all records in the first sync are included in the second sync
        Verify that the sync only sent records to the target for selected streams (catalogs)

        PREREQUISITE
        For EACH stream that is fully replicated there are multiple rows of data with
            different values for the replication key
        """
        conn_id = self.create_connection_with_initial_discovery()

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        full_streams = {key for key, value in self.expected_replication_method().items()
                        if value == self.FULL}
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in full_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), full_streams,
                         logging="verify only full table streams were replicated")

        first_sync_state = menagerie.get_state(conn_id)

        # Get the set of records from a first sync
        first_sync_records_by_stream = runner.get_records_from_target_output()

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # Get the set of records from a second sync
        second_sync_records_by_stream = runner.get_records_from_target_output()
        for stream in full_streams:
            with self.subTest(stream=stream):

                # verify there is no bookmark values from state
                state_value = first_sync_state.get("bookmarks", {}).get(stream)
                self.assertIsNone(state_value, logging="verify no bookmark value is saved in state")

                # verify that there is more than 1 record of data - setup necessary
                self.assertGreater(first_sync_record_count.get(stream, 0), 1,
                                   logging="verify multiple records are replicatied")

                # verify that you get the same or more data the 2nd time around
                self.assertGreaterEqual(
                    second_sync_record_count.get(stream, 0),
                    first_sync_record_count.get(stream, 0),
                    logging="verify the second full table sync replicates at least as many records as the first sync")

                # verify all data from 1st sync included in 2nd sync
                first_sync_records = [record["data"] for record in first_sync_records_by_stream[stream]["messages"]]
                second_sync_records = [record["data"] for record in second_sync_records_by_stream[stream]["messages"]]
                LOGGER.info("verify all records from the first sync are replicated in the second sync")
                for record in first_sync_records:
                    self.assertIn(record, second_sync_records)
