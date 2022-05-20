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
        streams_to_test = self.expected_streams() - {
            'projects', # BUG https://jira.talendforge.org/browse/TDL-19163
            'versions', # child of projects
            'components', # child of projects
        }

        conn_id = self.create_connection_with_initial_discovery()

        self.create_test_data()

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog['tap_stream_id'] in streams_to_test]
        self.select_all_streams_and_fields(conn_id, test_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()

        synced_recs = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                expected_pks = self.expected_primary_keys()[stream]

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

                pk_value_list = [
                    tuple(message.get("data").get(pk) for pk in expected_pks)
                    for message in synced_recs[stream].get("messages", [])
                    if message["action"] == "upsert"
                ]
                unique_pk_values = set(pk_value_list)

                # verify No records have dulpicate primary-keys value
                self.assertEqual(len(pk_value_list), len(unique_pk_values), msg="Replicated records does not have unique values.")
