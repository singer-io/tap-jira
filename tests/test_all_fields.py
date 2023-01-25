"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie
from tap_tester.logger import LOGGER
from base import BaseTapTest

class AllFieldsTest(BaseTapTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    # fields for which data is not generated or available via updating API Call
    fields_to_remove = {
        # Most of the fields will be included using "expand" parameter in the API Call
        # See backlog card: https://jira.talendforge.org/browse/TDL-17948 for more details
        "worklogs": ["properties"],
        # removed in the Tap
        "project_types": ["icon"],
        # name, key: properties are deprected
        # expand: not populated in the response
        "users": ["name", "key", "expand"],
        # project: not populated in the response
        "versions": ["expand", "moveUnfixedIssuesTo", "project", "remotelinks", "operations"],
        # fieldsToInclude: not found in the doc
        "issues": ["renderedFields", "schema", "editmeta", "fieldsToInclude", "versionedRepresentations", "names", "properties"],
        "issue_comments": ["properties", "renderedBody"],
        "projects": ["roles", "issueTypes", "email", "assigneeType", "components"],
        "issue_transitions": ["fields", "expand"],
        # iconUrl: not found in the doc
        "resolutions": ["iconUrl"],
        # historyMetadata started showing up 01/24/2023 so commenting this out for now
        # "changelogs": ["historyMetadata"]
    }

    @staticmethod
    def name():
        return "tt_jira_all_fields_test"

    def test_run(self):

        conn_id = self.create_connection_with_initial_discovery()

        self.create_test_data()

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)

        expected_streams = self.expected_streams()
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in expected_streams]

        # stream and field selection
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # grab metadata after performing table-and-field selection to set expectations
        stream_to_all_catalog_fields = dict() # used for asserting all fields are replicated
        for catalog in found_catalogs:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(
            expected_streams, synced_stream_names,
            logging=f"verify no unexpected streams are replicated: {expected_streams}"
        )

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # expected values
                expected_automatic_keys = self.expected_primary_keys().get(stream, set()) | \
                    self.top_level_replication_key_fields().get(stream, set()) | self.expected_foreign_keys().get(stream, set())
                # get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                # collect actual values
                messages = synced_records.get(stream)

                actual_all_keys = set()
                # collect actual values
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0,
                                   logging="verify at least 1 record was replicated")

                # verify all fields for a stream were replicated
                self.assertGreater(
                    len(expected_all_keys), len(expected_automatic_keys),
                    logging="verify more than just the automatic fields are replicated")
                self.assertTrue(
                    expected_automatic_keys.issubset(expected_all_keys),
                    msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"',
                    logging="verify the automatic fields are included on the records"
                )

                # remove some fields as data cannot be generated
                fields = self.fields_to_remove.get(stream) or []
                for field in fields:
                    LOGGER.info("removing field '%s' from expectations", field)
                    expected_all_keys.remove(field)
                self.assertSetEqual(
                    expected_all_keys, actual_all_keys,
                    logging="verify all discovered fields are replicated"
                )
