"""
Test tap sets a bookmark and respects it for the next sync of a stream
"""
from tap_tester import menagerie, runner
from tap_tester.logger import LOGGER

from base import BaseTapTest


class BookmarkTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""

    @staticmethod
    def name():
        return "tt_jira_bookmark_test"

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

        Verify that only data for incremental streams is sent to the target

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """
        conn_id = self.create_connection_with_initial_discovery()

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)

        # IF THERE ARE STREAMS THAT SHOULD NOT BE TESTED
        # REPLACE THE EMPTY SET BELOW WITH THOSE STREAMS
        untested_streams = self.child_streams().union({"issue_comments", "issue_transitions", "changelogs"})
        expected_streams = self.expected_streams().difference(untested_streams)

        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL}

        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in expected_streams]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=False)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), expected_streams)

        first_sync_state = menagerie.get_state(conn_id)

        # Get data about actual rows synced
        first_sync_records = runner.get_records_from_target_output()
        first_max_bookmarks = self.max_bookmarks_by_stream(first_sync_records)
        first_min_bookmarks = self.min_bookmarks_by_stream(first_sync_records)

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)
        second_sync_state = menagerie.get_state(conn_id)

        # Get data about rows synced
        second_sync_records = runner.get_records_from_target_output()
        second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_records)

        # THIS MAKES AN ASSUMPTION THAT CHILD STREAMS DO NOT HAVE BOOKMARKS.
        # ADJUST IF NECESSARY
        for stream in expected_streams:
            with self.subTest(stream=stream):

                if stream in incremental_streams:

                    # get bookmark values from state and target data
                    stream_bookmark_key = self.expected_replication_keys().get(stream, set())
                    stream_bookmark_key = stream_bookmark_key.pop()
                    state_value = first_sync_state.get("bookmarks", {}).get(
                        stream, {None: None}).get(stream_bookmark_key)
                    target_value = first_max_bookmarks.get(
                        stream, {None: None}).get(stream_bookmark_key)
                    target_min_value = first_min_bookmarks.get(
                        stream, {None: None}).get(stream_bookmark_key)

                    # verify that there is data with different bookmark values - setup necessary
                    self.assertGreaterEqual(
                        target_value, target_min_value,
                        msg="Data isn't set up to be able to test bookmarks",
                        logging="verify test data is setup with different replication key value"
                    )

                    # verify state agrees with target data after 1st sync
                    self.assertEqual(
                        state_value, target_value,
                        msg="The bookmark value isn't correct based on target data",
                        logging="verify the max replication key value is saved in state for the first sync"
                    )

                    # verify that you get less data the 2nd time around
                    self.assertGreater(
                        first_sync_record_count.get(stream, 0),
                        second_sync_record_count.get(stream, 0),
                        msg="second syc didn't have less records, bookmark usage not verified",
                        logging="verify less data is replicated on the second sync")

                    # verify all data from 2nd sync >= 1st bookmark
                    target_value = second_min_bookmarks.get(
                        stream, {None: None}).get(stream_bookmark_key)

                    # verify that the minimum bookmark sent to the target for the second sync
                    # is greater than or equal to the bookmark from the first sync
                    self.assertGreaterEqual(
                        target_value, state_value,
                        logging="verify the second sync replicates records inclusive of the last saved bookmark"
                    )

                else:

                    first_bookmark_key_value = first_sync_state.get('bookmarks', {stream: None}).get(stream)
                    second_bookmark_key_value = second_sync_state.get('bookmarks', {stream: None}).get(stream)
                    first_sync_count = first_sync_record_count.get(stream, 0)
                    second_sync_count = second_sync_record_count.get(stream, 0)

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value,
                                      logging="verify a bookmark is not saved in state")
                    self.assertIsNone(second_bookmark_key_value,
                                      logging="verify a bookmark is not saved in state")

                    # Verify data is replicated for each sync (ensuring test coverage)
                    self.assertGreater(first_sync_count, 0,
                                       logging="verify records are replicated for the first sync")
                    self.assertGreater(second_sync_count, 0,
                                       logging="verify records are replicated for the second sync")

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(first_sync_count, second_sync_count,
                                     logging="verify the same number of records are replicated for each sync")
