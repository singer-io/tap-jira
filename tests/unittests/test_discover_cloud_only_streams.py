import unittest
from unittest import mock

import tap_jira
from tap_jira import streams


class TestDiscoverCloudOnlyStreams(unittest.TestCase):
    '''
        Verify `discover()` excludes cloud-only streams (`groups`,
        `group_users`, backed by the Cloud-only /rest/api/2/group/bulk
        endpoint) from the catalog for on-prem instances, while still
        including them for Cloud instances.
    '''

    def setUp(self):
        # `generate_metadata` mutates the shared `users` stream's pk_fields
        # in place when it sees an on-prem client; snapshot/restore it so
        # this test doesn't leak state into other test modules.
        users_stream = next(s for s in streams.ALL_STREAMS if s.tap_stream_id == "users")
        self._users_stream = users_stream
        self._original_users_pk_fields = list(users_stream.pk_fields)

    def tearDown(self):
        self._users_stream.pk_fields = self._original_users_pk_fields

    @mock.patch("tap_jira.Context.client")
    def test_discover_excludes_cloud_only_streams_for_on_prem(self, mock_client):
        mock_client.is_on_prem_instance = True

        catalog = tap_jira.discover()
        stream_ids = {s.tap_stream_id for s in catalog.streams}

        self.assertNotIn("groups", stream_ids)
        self.assertNotIn("group_users", stream_ids)
        # Non cloud-only streams should still be present
        self.assertIn("projects", stream_ids)
        self.assertIn("users", stream_ids)

    @mock.patch("tap_jira.Context.client")
    def test_discover_includes_cloud_only_streams_for_cloud(self, mock_client):
        mock_client.is_on_prem_instance = False

        catalog = tap_jira.discover()
        stream_ids = {s.tap_stream_id for s in catalog.streams}

        self.assertIn("groups", stream_ids)
        self.assertIn("group_users", stream_ids)
