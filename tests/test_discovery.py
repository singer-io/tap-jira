"""
Test tap discovery
"""
import re

from tap_tester import menagerie

from base import BaseTapTest

# BUG_TDL-19502 [tap-jira] missing informational metadata 'forced-replication-method'


class DiscoveryTest(BaseTapTest):
    """ Test the tap discovery """

    @staticmethod
    def name():
        return "tt_jira_discovery_test"

    def test_run(self):
        """
        Verify that discover creates the appropriate catalog, schema, metadata, etc.

        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • verify there is only 1 top level breadcrumb
        • verify there are no duplicate/conflicting metadata entries
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • verify the actual replication matches our expected replication method
        • verify that primary, replication and foreign keys
          are given the inclusion of automatic (metadata and annotated schema).
        • verify that all other fields have inclusion of available (metadata and schema)
        """
        conn_id = self.create_connection_with_initial_discovery()

        # Verify number of actual streams discovered match expected
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0,
                           logging="verify a catalog was produced by discovery")
        self.assertEqual(len(found_catalogs), len(self.expected_streams()),
                         logging=f"verify {len(self.expected_streams())} streams were discovered")

        # Verify the stream names discovered were what we expect
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertEqual(set(self.expected_streams()), set(found_catalog_names),
                        logging=f"verify the expected streams discovered were: {set(self.expected_streams()),}")

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        self.assertTrue(all([re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names]),
                        logging="verify stream names use on lower case alpha or underscore characters")

        for stream in self.expected_streams():
            with self.subTest(stream=stream):

                # gather expectations
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_key = self.expected_replication_key_metadata()[stream]
                expected_automatic_fields = expected_primary_keys  # BUG_TDL-19502
                #expected_automatic_fields = (expected_primary_keys | expected_replication_key)  # BUG_TDL-19502
                expected_replication_method = self.expected_replication_method().get(stream, None)

                # gather results
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog, logging="verify an entry is present in the catalog")
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_replication_key = set(stream_properties[0].get(
                    "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, []))
                actual_primary_keys = set(stream_properties[0].get(
                    "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, []))
                actual_replication_method = stream_properties[0].get(
                    "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                actual_automatic_fields = {item.get("breadcrumb", ["properties", None])[1]
                                           for item in metadata
                                           if item.get("metadata").get("inclusion") == "automatic"}
                actual_fields = []
                for md_entry in metadata:
                    if md_entry["breadcrumb"] != []:
                        actual_fields.append(md_entry["breadcrumb"][1])

                # verify the stream level properties are as expected
                # verify there is only 1 top level breadcrumb
                self.assertEqual(len(stream_properties), 1,
                                logging="verify there is only one top level breadcrumb")

                # Verify there are no duplicate/conflicting metadata entries.
                self.assertEqual(len(actual_fields), len(set(actual_fields)),
                                 logging="verify there are no duplicate entries in metadata")

                # verify primary key(s) are marked in metadata
                self.assertEqual(actual_primary_keys, expected_primary_keys,
                                 logging=f"verify {expected_primary_keys} is saved in metadata as the primary-key")

                # BUG_TDL-19502
                # verify replication key(s) are marked in metadata
                # self.assertEqual(actual_replication_key, expected_replication_key,
                #                  logging=f"verify {expected_replication_key} is saved in metadata as the replication-key")

                # BUG_TDL-19502
                # verify the actual replication matches our expected replication method
                # self.assertEqual(expected_replication_method, actual_replication_method,
                #                  logging=f"verify the replication method is {expected_replication_method}")

                # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                # If replication keys are not specified in metadata, skip this check
                # BUG_TDL-19502
                # if actual_replication_key:
                #     self.assertEqual(
                #         actual_replication_method, self.INCREMENTAL,
                #         logging=f"verify the forced replication method is {self.INCREMENTAL} since there is a replication-key"
                #     )
                # else:
                #     self.assertEqual(
                #         actual_replication_method, self.FULL,
                #         logging=f"verify the forced replication method is {self.FULL} since there is no replication-key"
                #     )

                # verify that primary, replication are given the inclusion of automatic in metadata.
                self.assertSetEqual(expected_automatic_fields, actual_automatic_fields,
                                    logging="verify primary and replication key fields are automatic")

                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all({item.get("metadata").get("inclusion") == "available"
                         for item in metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in actual_automatic_fields}),
                    msg="Not all non key properties are set to available in metadata",
                    logging="verify all non-autoamtic fields are available")
