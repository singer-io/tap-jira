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
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"

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
            'start_date': '2017-07-01T00:00:00Z',
            'username': os.getenv(properties_env["username"]),
            'base_url': os.getenv(properties_env["base_url"])
        }

        if original:
            return return_value

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
            self.API_LIMIT: 0,
        }

        key_pk = {
            self.PRIMARY_KEYS: {"key"},
            self.API_LIMIT: 0,
        }

        return {
            "projects": id_pk, # Tap uses deprecated all projects endpoint, not paginated one, see https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-projects/#api-group-projects
            "project_types": key_pk, # Not paginated, see https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-project-types/#api-rest-api-2-project-type-get
            "project_categories": id_pk, # Not paginated, see https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-project-categories/
            "versions": {
                self.PRIMARY_KEYS: {"id"},
                self.API_LIMIT: 0, # TODO: Backlog ticket to create data required to test this 50 - maxResults comes back as this
                # https://stitchdata.atlassian.net/browse/SRCE-5193
            },
            "components": {
                self.PRIMARY_KEYS: {"id"},
                self.API_LIMIT: 50, # maxResults comes back as this
            },
            "resolutions": id_pk, # Not paginated, see https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-issue-resolutions/#api-group-issue-resolutions
            "roles": id_pk, # Not paginated, see https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-project-roles/#api-rest-api-2-role-get
            "users": {
                self.PRIMARY_KEYS: {"accountId"},
                self.API_LIMIT: 2, # - maxResults comes back as this
            },
            "issues": {
                self.PRIMARY_KEYS: {"id"},
                self.API_LIMIT: 0, # TODO: Backlog ticket to create data required to test this 50 - maxResults comes back as this
                # https://stitchdata.atlassian.net/browse/SRCE-5193
            },
            "issue_comments": id_pk, # Returned as part of the issue
            "issue_transitions": id_pk, # Returned as part of the issue
            "changelogs": id_pk, # Returned as part of the issue
            "worklogs": {
                self.PRIMARY_KEYS: {"id"},
                self.API_LIMIT: 0, # TODO: Backlog ticket to create data required to test this documentation says 1000, see https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-issue-worklogs/#api-rest-api-2-worklog-updated-get
                # https://stitchdata.atlassian.net/browse/SRCE-5193
            },
        }
