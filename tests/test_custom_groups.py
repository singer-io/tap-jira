"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie
from base import BaseTapTest

class CustomGroupsTest(BaseTapTest):
    """Test that custom groups are used if provided"""

    @staticmethod
    def name():
        return "tt_jira_custom_fields_test"

    def get_properties(self, original: bool = True):
        return_value = super().get_properties(original)
        if not original:
            return_value["groups"] = "doesnotexist,reallydoesnotexist"
        return return_value

    def test_run(self):
        """
        Test assumes that the test account has at least one user that doesn't exist
        in the groups returned by the get_properties function above
        """
        # create a connection with groups
        conn_id_1 = self.create_connection_with_initial_discovery(original_properties=False)

        # select the users stream and all it's fields
        found_catalogs_1 = menagerie.get_catalogs(conn_id_1)
        user_catalog_1 = list(filter(lambda x: x["stream_name"] == "users", found_catalogs_1))
        self.select_all_streams_and_fields(conn_id_1, user_catalog_1, select_all_fields=True)

        # Run a sync job using orchestrator
        record_count_by_stream_1 = self.run_sync(conn_id_1)

        # create a connection without any groups specified
        conn_id_2 = self.create_connection_with_initial_discovery()

        # select the users stream and all it's fields
        found_catalogs_2 = menagerie.get_catalogs(conn_id_2)
        user_catalog_2 = list(filter(lambda x: x["stream_name"] == "users", found_catalogs_2))
        self.select_all_streams_and_fields(conn_id_2, user_catalog_2, select_all_fields=True)

        # Run a sync job using orchestrator
        record_count_by_stream_2 = self.run_sync(conn_id_2)

        # verify users exist in our test account
        self.assertGreater(
            record_count_by_stream_2.get("users", 0), 0,
            logging="verify users data exists in our test account"
        )

        # verify users records are filtered by the groups property
        self.assertEqual(
            record_count_by_stream_1.get("users", 0), 0,
            logging="verify no users records are replicated when non-existent groups are set"
        )
