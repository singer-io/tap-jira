import unittest
from unittest import mock
from types import SimpleNamespace

from tap_jira import streams


class TestGroupUsersStreamRegistration(unittest.TestCase):
    '''Verify `group_users` is registered as an indirect child stream of `groups`.'''

    def test_group_users_stream_present_in_all_streams(self):
        group_users_streams = [s for s in streams.ALL_STREAMS if s.tap_stream_id == "group_users"]
        self.assertEqual(len(group_users_streams), 1)

        group_users_stream = group_users_streams[0]
        self.assertIsInstance(group_users_stream, streams.Stream)
        self.assertEqual(group_users_stream.pk_fields, ["groupId", "accountId"])
        self.assertEqual(group_users_stream.forced_replication_method, "FULL_TABLE")
        self.assertTrue(group_users_stream.cloud_only)

    def test_group_users_is_an_indirect_child_of_groups(self):
        # `group_users` data is only produced via `groups`.sync(), so the
        # main sync loop must skip it directly and it must declare `groups`
        # as its parent.
        self.assertTrue(streams.GROUP_USERS.indirect_stream)
        self.assertEqual(streams.GROUP_USERS.parent_tap_stream_id, "groups")


class TestGroupUsersDependency(unittest.TestCase):
    '''Verify selecting `group_users` without `groups` raises a dependency error.'''

    def test_validate_dependencies_raises_when_group_users_selected_without_groups(self):
        stream_ids = [s.tap_stream_id for s in streams.ALL_STREAMS]
        fake_catalog = SimpleNamespace(
            streams=[SimpleNamespace(tap_stream_id=sid) for sid in stream_ids]
        )

        with mock.patch("tap_jira.streams.Context.catalog", fake_catalog), \
             mock.patch("tap_jira.streams.Context.is_selected",
                        side_effect=lambda sid: sid == "group_users"):
            with self.assertRaises(streams.DependencyException):
                streams.validate_dependencies()

    def test_validate_dependencies_allows_group_users_with_groups(self):
        stream_ids = [s.tap_stream_id for s in streams.ALL_STREAMS]
        fake_catalog = SimpleNamespace(
            streams=[SimpleNamespace(tap_stream_id=sid) for sid in stream_ids]
        )

        with mock.patch("tap_jira.streams.Context.catalog", fake_catalog), \
             mock.patch("tap_jira.streams.Context.is_selected",
                        side_effect=lambda sid: sid in ("group_users", "groups")):
            # Should not raise since `groups` is also selected.
            streams.validate_dependencies()
