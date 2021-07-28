"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie
from base import BaseTapTest

class CustomGroupsTest(BaseTapTest):
    """Test that custom groups are used if provided"""

    def name(self):
        return "tt_jira_custom_fields_test"

    def get_properties(self, original: bool = True):
        return_value = super().get_properties(original)
        return_value["groups"] = "doesnotexist,reallydoesnotexist"
        return return_value

    def test_run(self):
        """
        Test assumes that the test account has at least one user that doesn't exist
        in the groups returned by the get_properties function above
        """
        conn_id = self.create_connection_with_initial_discovery()

        found_catalogs = menagerie.get_catalogs(conn_id)
        user_catalog = list(filter(lambda x: x["stream_name"] == "users", found_catalogs))

        self.select_all_streams_and_fields(conn_id, user_catalog, select_all_fields=False)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_sync(conn_id)

        self.assertEqual(
            record_count_by_stream.get("users", 0),
            0,
            msg="Expected zero users records because we used garbage group names")
