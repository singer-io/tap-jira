"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie
from base import BaseTapTest

class AllFieldsTest(BaseTapTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    # fields for which data is not generated
    fields_to_remove = {
        "worklogs": ["properties", "visibility"],
        "project_types": ["icon"],
        "users": ["name", "key", "expand"],
        "versions": ["expand", "moveUnfixedIssuesTo", "project", "remotelinks", "operations"],
        "issues": ["renderedFields", "schema", "editmeta", "fieldsToInclude", "versionedRepresentations", "names", "properties"],
        "issue_comments": ["properties", "visibility", "renderedBody"],
        "projects": ["roles", "url", "issueTypes", "email", "assigneeType", "components", "projectCategory"],
        "issue_transitions": ["fields", "expand"],
        "resolutions": ["iconUrl"],
        "changelogs": ["historyMetadata"]
    }

    def name(self):
        return "tt_jira_all_fields_test"

    def test_run(self):

        conn_id = self.create_connection_with_initial_discovery()

        self.create_test_data()

        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.select_all_streams_and_fields(conn_id, found_catalogs, select_all_fields=True)

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
        self.assertSetEqual(self.expected_streams(), synced_stream_names)

        for stream in self.expected_streams():
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
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(expected_all_keys), msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"')

                # remove some fields as data cannot be generated
                fields = self.fields_to_remove.get(stream) or []
                for field in fields:
                    expected_all_keys.remove(field)

                self.assertSetEqual(expected_all_keys, actual_all_keys)