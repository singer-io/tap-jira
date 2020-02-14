import os

class TapSpec():
    """ Base class to specify tap-specific configuration. """

    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    API_LIMIT = "max-row-limit"
    START_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    CONFIGURATION_ENVIRONMENT = {
        "properties": {
            'username': 'TAP_JIRA_USERNAME',
            'base_url': 'TAP_JIRA_BASE_URL'
        },
        "credentials": {
            'password' : 'TAP_JIRA_PASSWORD'
        }
    }

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-jira"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.jira"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        properties_env = self.CONFIGURATION_ENVIRONMENT['properties']
        return_value = {
            'start_date': '2017-07-01 00:00:00',
            'username': os.getenv(properties_env["username"]),
            'base_url': os.getenv(properties_env["base_url"])
        }

        if original:
            return return_value

        # This test needs the new connections start date to be larger than the default
        assert self.start_date > return_value["start_date"]

        return_value["start_date"] = self.start_date
        return return_value

    def get_credentials(self):
        """Authentication information for the test account"""
        credentials_env = self.CONFIGURATION_ENVIRONMENT['credentials']
        return {
            'password': os.getenv(credentials_env["password"])
        }

    @staticmethod
    def forced_incremental_streams():
        # "key" refers to the path in state and "path" refers to the path in the record
        return {"issues": {"bookmark_path": "updated", "record_path": ("fields", "updated")}, "worklogs": "updated"}

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        id_pk = {
            self.PRIMARY_KEYS: {"id"},
        }

        key_pk = {
            self.PRIMARY_KEYS: {"key"},
        }

        account_id_pk = {
            self.PRIMARY_KEYS: {"accountId"},
        }

        return {
            "projects": id_pk,
            "project_types": key_pk,
            "project_categories": id_pk,
            "versions": id_pk,
            "resolutions": id_pk,
            "roles": id_pk,
            "users": account_id_pk,
            "issues": id_pk,
            "issue_comments": id_pk,
            "issue_transitions": id_pk,
            "changelogs": id_pk,
            "worklogs": id_pk,
        }
