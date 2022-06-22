"""
Test tap pagination of streams
"""
from tap_tester import menagerie, runner

from base import BaseTapTest


class PaginationTest(BaseTapTest):
    """ Test the tap pagination to get multiple pages of data """

    @staticmethod
    def name():
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
      
        synced_recs = runner.get_records_from_target_output()

        for stream in self.expected_streams():
            with self.subTest(stream=stream):

                # gather expectations
                expected_pks = self.expected_primary_keys()[stream]

                # gather results
                record_count = record_count_by_stream.get(stream, -1)
                api_limit = self.expected_metadata().get(stream, {}).get(self.API_LIMIT)
                replicated_fields = actual_fields_by_stream.get(stream, set())
                pk_value_list = [
                    tuple(message.get("data").get(pk) for pk in expected_pks)
                    for message in synced_recs[stream].get("messages", [])
                    if message["action"] == "upsert"
                ]
                unique_pk_values = set(pk_value_list)
                
                # verify that we can paginate with all fields selected
                self.assertGreater(
                    record_count, api_limit,
                    logging="verify the number of records replicated exceeds the stream api limit"
                )

                # verify that the automatic fields are sent to the target
                self.assertTrue(
                    replicated_fields.issuperset(
                        self.expected_primary_keys().get(stream, set()) |
                        self.top_level_replication_key_fields().get(stream, set()) |
                        self.expected_foreign_keys().get(stream, set())),
                    logging="verify the automatic fields are sent to the target"
                )

                # verify we have more fields sent to the target than just automatic fields
                # SKIP THIS ASSERTION IF ALL FIELDS ARE INTENTIONALLY AUTOMATIC FOR THIS STREAM
                self.assertTrue(
                    replicated_fields.difference(
                        self.expected_primary_keys().get(stream, set()) |
                        self.expected_replication_keys().get(stream, set())
                    ),
                    logging="verify more than just the automatic fields are sent to the target"
                )

                # verify no records have dulpicate primary-keys value
                self.assertEqual(len(pk_value_list), len(unique_pk_values),
                                 logging="verify records have unique primary key values")
