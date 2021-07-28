"""
Test tap pagination of streams
"""
from tap_tester import menagerie, runner

from base import BaseTapTest


class PaginationTest(BaseTapTest):
    """ Test the tap pagination to get multiple pages of data """

    def name(self):
        return "tt_jira_pagination_test"

    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        and that when all fields are selected more than the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        conn_id = self.create_connection_with_initial_discovery()

        self.create_test_data()

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.select_all_streams_and_fields(conn_id, found_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()


        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                # verify that we can paginate with all fields selected
                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    self.expected_metadata().get(stream, {}).get(self.API_LIMIT),
                    msg="The number of records is not over the stream max limit")

                # verify that the automatic fields are sent to the target
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).issuperset(
                        self.expected_primary_keys().get(stream, set()) |
                        self.top_level_replication_key_fields().get(stream, set()) |
                        self.expected_foreign_keys().get(stream, set())),
                    msg="The fields sent to the target don't include all automatic fields"
                )

                # verify we have more fields sent to the target than just automatic fields
                # SKIP THIS ASSERTION IF ALL FIELDS ARE INTENTIONALLY AUTOMATIC FOR THIS STREAM
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).symmetric_difference(
                        self.expected_primary_keys().get(stream, set()) |
                        self.expected_replication_keys().get(stream, set()) |
                        self.expected_foreign_keys().get(stream, set())),
                    msg="The fields sent to the target don't include non-automatic fields"
                )
