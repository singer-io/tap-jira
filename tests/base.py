"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import os
from datetime import datetime as dt
from datetime import timezone as tz

from tap_tester import connections, menagerie, runner
from tap_tester.base_case import BaseCase
from tap_tester.logger import LOGGER

from test_client import TestClient, ALL_TEST_STREAMS


class BaseTapTest(BaseCase):
    """
    Setup expectations for test sub classes
    Run discovery for as a prerequisite for most tests
    """

    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"

    API_LIMIT = "max-row-limit"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    PARENT_STREAM = "parent-stream"

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
            "projects": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL,
                self.API_LIMIT: 50,
            }, # maxResults comes back as this
            "project_types": {
                self.PRIMARY_KEYS: {"key"},
                self.REPLICATION_METHOD: self.FULL,
                self.API_LIMIT: 0, # Not paginated, see https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-project-types/#api-rest-api-2-project-type-get
            },
            "project_categories": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL,
                self.API_LIMIT: 0,
            }, # Not paginated, see https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-project-categories/
            "versions": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL,
                self.API_LIMIT: 0, # TODO: Backlog ticket to create data required to test this 50 - maxResults comes back as this
                # https://stitchdata.atlassian.net/browse/SRCE-5193
            },
            "components": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL,
                self.API_LIMIT: 50, # maxResults comes back as this
            },
            "resolutions": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL,
                self.API_LIMIT: 0,
            }, # Not paginated, see https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-issue-resolutions/#api-group-issue-resolutions
            "roles": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL,
                self.API_LIMIT: 0,
            }, # Not paginated, see https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-project-roles/#api-rest-api-2-role-get
            "users": {
                self.PRIMARY_KEYS: {"accountId"},
                self.REPLICATION_METHOD: self.FULL,
                self.API_LIMIT: 2, # - maxResults comes back as this
            },
            "issues": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated"},
                self.API_LIMIT: 0, # TODO: Backlog ticket to create data required to test this 50 - maxResults comes back as this
                # https://stitchdata.atlassian.net/browse/SRCE-5193
            },
            "issue_comments": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: 0,
                self.PARENT_STREAM: "issues",
            }, # Returned as part of the issue
            "issue_transitions": {
                self.PRIMARY_KEYS: {"id","issueId"},# Composite primary key
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: 0,
                self.PARENT_STREAM: "issues",
            }, # Returned as part of the issue
            "changelogs": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: 0,
                self.PARENT_STREAM: "issues",
            }, # Returned as part of the issue
            "worklogs": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.API_LIMIT: 0, # TODO: Backlog ticket to create data required to test this documentation says 1000, see https://developer.atlassian.com/cloud/jira/platform/rest/v2/api-group-issue-worklogs/#api-rest-api-2-worklog-updated-get
                # https://stitchdata.atlassian.net/browse/SRCE-5193
            },
        }

    def environment_variables(self):
        return ({p for p in self.CONFIGURATION_ENVIRONMENT['properties'].values()} |
                {c for c in self.CONFIGURATION_ENVIRONMENT['credentials'].values()})

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def child_streams(self):
        """
        Return a set of streams that are child streams
        based on having foreign key metadata
        """
        return {stream
                for stream, metadata in self.expected_metadata().items()
                if metadata.get(self.PARENT_STREAM)}

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_key_metadata(self):
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {**self.expected_replication_key_metadata(),
                **{table: {key} for table, key in self.forced_incremental_streams().items()
                   if isinstance(key, str)},
                **{table: {key["bookmark_path"]} for table, key in self.forced_incremental_streams().items()
                   if isinstance(key, dict)}}

    def expected_replication_key_record_paths(self):
        return {**self.expected_replication_key_metadata(),
                **{table: {key} for table, key in self.forced_incremental_streams().items()
                   if isinstance(key, str)},
                **{table: {key["record_path"]} for table, key in self.forced_incremental_streams().items()
                   if isinstance(key, dict)}}

    def expected_foreign_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of foreign key fields
        """
        return {table: properties.get(self.FOREIGN_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        """return a dictionary with key of table name and value of replication method"""
        return {**{table: properties.get(self.REPLICATION_METHOD, None)
                 for table, properties
                 in self.expected_metadata().items()},
                **{key: self.INCREMENTAL for key in self.forced_incremental_streams().keys()}}

    def top_level_replication_key_fields(self):
        """ This is the set of automatically selected top-level replication key fields emitted with each record. """
        top_level_fields = {}
        for stream, fields in self.expected_replication_key_record_paths().items():
            peek = lambda s: next(iter(s))
            if (len(fields) == 1 and
                isinstance(peek(fields), tuple)):
                fields = {fields.pop()[0]}
            top_level_fields[stream] = fields
        return top_level_fields

    def setUp(self):
        """Verify that you have set the prerequisites to run the tap (creds, etc.)"""
        missing_envs = [x for x in self.environment_variables() if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing test-required environment variables: {}".format(missing_envs))

    #########################
    #   Helper Methods      #
    #########################

    def create_connection_with_initial_discovery(self, original_properties: bool = True):
        """Create a new connection with the test name"""
        # Create the connection
        conn_id = connections.ensure_connection(self, original_properties)

        # Run a check job using orchestrator (discovery)
        check_job_name = runner.run_check_mode(self, conn_id)

        # Assert that the check job succeeded
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
        return conn_id

    def run_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())


        return sync_record_count

    @staticmethod
    def local_to_utc(date: dt):
        """Convert a datetime with timezone information to utc"""
        utc = dt(date.year, date.month, date.day, date.hour, date.minute,
                 date.second, date.microsecond, tz.utc)

        if date.tzinfo and hasattr(date.tzinfo, "_offset"):
            utc += date.tzinfo._offset

        return utc

    def max_bookmarks_by_stream(self, sync_records):
        """
        Return the maximum value for the replication key for each stream
        which is the bookmark expected value.

        Comparisons are based on the class of the bookmark value. Dates will be
        string compared which works for ISO date-time strings
        """
        max_bookmarks = {}
        # get incremental streams
        incremental_streams = [key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL]
        for stream, batch in sync_records.items():
            # skip stream that is not incremental
            # as bookmark will be written for incremental streams
            if stream not in incremental_streams:
                continue

            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']
            stream_record_key = self.expected_replication_key_record_paths().get(stream, set())
            stream_bookmark_key = self.expected_replication_keys().get(stream, set())

            self.assertEqual(len(stream_bookmark_key), 1, msg="Stream {} has a bookmark key that is not of length 1.\nCompound replication keys and empty replication keys are not supported by max_boomarks.\nBookmark Key: {}".format(stream, stream_bookmark_key))
            stream_bookmark_key = stream_bookmark_key.pop()
            stream_record_key = stream_record_key.pop()

            def get_bookmark(message_data, stream_record_key):
                if isinstance(stream_record_key, tuple):
                    if len(stream_record_key) == 1:
                        return message_data[stream_record_key[0]]
                    return get_bookmark(message_data[stream_record_key[0]],
                                        stream_record_key[1:])
                return message_data[stream_record_key]

            bk_values = [get_bookmark(message['data'], stream_record_key) for message in upsert_messages]
            max_bookmarks[stream] = {stream_bookmark_key: None}
            for bk_value in bk_values:
                if bk_value is None:
                    continue

                if max_bookmarks[stream][stream_bookmark_key] is None:
                    max_bookmarks[stream][stream_bookmark_key] = bk_value

                if bk_value > max_bookmarks[stream][stream_bookmark_key]:
                    max_bookmarks[stream][stream_bookmark_key] = bk_value
        return max_bookmarks

    def min_bookmarks_by_stream(self, sync_records):
        """Return the minimum value for the replication key for each stream"""
        min_bookmarks = {}
        # get incremental streams
        incremental_streams = [key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL]
        for stream, batch in sync_records.items():
            # skip stream that is not incremental
            # as bookmark will be written for incremental streams
            if stream not in incremental_streams:
                continue

            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']
            stream_record_key = self.expected_replication_key_record_paths().get(stream, set())
            stream_bookmark_key = self.expected_replication_keys().get(stream, set())

            self.assertEqual(len(stream_bookmark_key), 1, msg="Stream {} has a bookmark key that is not of length 1.\nCompound replication keys and empty replication keys are not supported by min_boomarks.\nBookmark Key: {}".format(stream, stream_bookmark_key))
            stream_bookmark_key = stream_bookmark_key.pop()
            stream_record_key = stream_record_key.pop()

            def get_bookmark(message_data, stream_record_key):
                if isinstance(stream_record_key, tuple):
                    if len(stream_record_key) == 1:
                        return message_data[stream_record_key[0]]
                    return get_bookmark(message_data[stream_record_key[0]],
                                        stream_record_key[1:])
                return message_data[stream_record_key]

            bk_values = [get_bookmark(message['data'], stream_record_key) for message in upsert_messages]
            min_bookmarks[stream] = {stream_bookmark_key: None}
            for bk_value in bk_values:
                if bk_value is None:
                    continue

                if min_bookmarks[stream][stream_bookmark_key] is None:
                    min_bookmarks[stream][stream_bookmark_key] = bk_value

                if bk_value < min_bookmarks[stream][stream_bookmark_key]:
                    min_bookmarks[stream][stream_bookmark_key] = bk_value
        return min_bookmarks

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = set(schema.get('annotated-schema', {}).get(
                    'properties', {}).keys())

            # HACK: This can be removed if the tap unwraps envelope
            # objects and declares replication keys as automatic
            if catalog["tap_stream_id"] == 'issues' and 'fields' in non_selected_properties:
                non_selected_properties.remove("fields") # This contains replication key for issues
            elif catalog["tap_stream_id"] == "worklogs" and 'updated' in non_selected_properties:
                non_selected_properties.remove("updated") # Replication key for worklogs

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = self.get_properties().get("start_date")
        self.client = TestClient({
            **self.get_properties(),
            **self.get_credentials(),
        })

    def create_test_data(self):
        for stream in self.expected_streams():
            # TODO: Once more streams have data creation implemented add them here
            if stream == 'components':
                api_limit = self.expected_metadata().get(stream, {}).get(self.API_LIMIT)
                ALL_TEST_STREAMS[stream](self.client).create_test_data(api_limit)
