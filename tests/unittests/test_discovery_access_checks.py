import unittest
from unittest import mock

import tap_jira
from tap_jira import streams
from tap_jira.http import JiraForbiddenError, JiraNotFoundError

ALL_IDS = [s.tap_stream_id for s in streams.ALL_STREAMS]
PARENT_IDS = [s.tap_stream_id for s in streams.ALL_STREAMS if not s.parent_tap_stream_id]


def forbidden(path_fragments):
    """Build a `client.request` side effect raising 403 for matching paths."""
    def _request(tap_stream_id, method, path, **kwargs):
        if any(fragment in path for fragment in path_fragments):
            raise JiraForbiddenError("HTTP-error-code: 403, Error: forbidden")
        return {}
    return _request


class DiscoveryAccessCheckBase(unittest.TestCase):
    def setUp(self):
        # `generate_metadata` mutates the shared `users` stream's pk_fields
        # when it sees an on-prem client; snapshot/restore it.
        users_stream = next(s for s in streams.ALL_STREAMS if s.tap_stream_id == "users")
        self._users_stream = users_stream
        self._original_users_pk_fields = list(users_stream.pk_fields)

        patcher = mock.patch("tap_jira.Context.client")
        self.mock_client = patcher.start()
        self.addCleanup(patcher.stop)
        self.mock_client.is_on_prem_instance = False

    def tearDown(self):
        self._users_stream.pk_fields = self._original_users_pk_fields

    def discover_ids(self):
        return {s.tap_stream_id for s in tap_jira.discover(self.mock_client).streams}


class TestDiscoveryAccessChecks(DiscoveryAccessCheckBase):
    def test_all_streams_accessible(self):
        """All streams are in the catalog when nothing is forbidden."""
        self.mock_client.request.return_value = {}
        self.assertEqual(self.discover_ids(), set(ALL_IDS))

    def test_forbidden_parent_is_excluded(self):
        """A forbidden parent stream is excluded; unrelated streams remain."""
        self.mock_client.request.side_effect = forbidden(["/rest/api/2/role"])
        ids = self.discover_ids()
        self.assertNotIn("roles", ids)
        self.assertIn("projects", ids)
        self.assertIn("issues", ids)
        self.assertEqual(len(ids), len(ALL_IDS) - 1)

    def test_forbidden_parent_excludes_children(self):
        """Children of a forbidden parent are excluded."""
        self.mock_client.request.side_effect = forbidden(["/rest/api/2/search/jql"])
        ids = self.discover_ids()
        for stream_id in ("issues", "issue_comments", "changelogs", "issue_transitions"):
            self.assertNotIn(stream_id, ids)
        self.assertIn("projects", ids)
        self.assertIn("versions", ids)

    def test_forbidden_projects_excludes_versions_and_components(self):
        self.mock_client.request.side_effect = forbidden(["/rest/api/2/project/search"])
        ids = self.discover_ids()
        for stream_id in ("projects", "versions", "components"):
            self.assertNotIn(stream_id, ids)
        self.assertIn("project_types", ids)

    def test_forbidden_groups_excludes_group_users(self):
        self.mock_client.request.side_effect = forbidden(["/rest/api/2/group/bulk"])
        ids = self.discover_ids()
        self.assertNotIn("groups", ids)
        self.assertNotIn("group_users", ids)

    def test_on_prem_projects_probe_uses_project_endpoint(self):
        self.mock_client.is_on_prem_instance = True
        self.mock_client.request.side_effect = forbidden(["/rest/api/2/project/search"])
        # /project/search is not used for on-prem, so projects stay accessible
        self.assertIn("projects", self.discover_ids())

    def test_no_accessible_streams_raises(self):
        """Discovery fails when every parent stream is forbidden."""
        self.mock_client.request.side_effect = forbidden(["/rest/api/"])
        with self.assertRaises(JiraForbiddenError):
            tap_jira.discover(self.mock_client)

    def test_non_forbidden_errors_propagate(self):
        """Only 403 excludes a stream; other errors still fail discovery."""
        self.mock_client.request.side_effect = RuntimeError("boom")
        with self.assertRaises(RuntimeError):
            tap_jira.discover(self.mock_client)


class TestCheckAccess(DiscoveryAccessCheckBase):
    def test_child_stream_always_accessible(self):
        # `group_users` is the one child that validates its own endpoint
        # (see TestGroupUsersDiscovery); every other child defers to its parent.
        self.mock_client.request.side_effect = forbidden(["/rest/api/"])
        for stream in streams.ALL_STREAMS:
            if stream.parent_tap_stream_id and stream.tap_stream_id != "group_users":
                self.assertTrue(stream.check_access(self.mock_client), stream.tap_stream_id)
        self.mock_client.request.assert_not_called()

    def test_parent_stream_forbidden_returns_false(self):
        self.mock_client.request.side_effect = forbidden(["/rest/api/"])
        for stream_id in PARENT_IDS:
            stream = next(s for s in streams.ALL_STREAMS if s.tap_stream_id == stream_id)
            self.assertFalse(stream.check_access(self.mock_client), stream_id)

    def test_parent_stream_accessible_returns_true(self):
        self.mock_client.request.return_value = {}
        for stream_id in PARENT_IDS:
            stream = next(s for s in streams.ALL_STREAMS if s.tap_stream_id == stream_id)
            self.assertTrue(stream.check_access(self.mock_client), stream_id)

    def test_issues_falls_back_when_search_jql_not_found(self):
        def _request(tap_stream_id, method, path, **kwargs):
            if path == "/rest/api/2/search/jql":
                raise JiraNotFoundError("HTTP-error-code: 404")
            return {}
        self.mock_client.request.side_effect = _request
        self.assertTrue(streams.ISSUES.check_access(self.mock_client))
        called_paths = [c.args[2] for c in self.mock_client.request.call_args_list]
        self.assertEqual(called_paths, ["/rest/api/2/search/jql", "/rest/api/2/search"])

    @mock.patch("tap_jira.streams.Context.config", {"groups": "a, b"})
    def test_users_skips_missing_groups_and_detects_forbidden(self):
        def _request(tap_stream_id, method, path, params=None, **kwargs):
            if params["groupname"] == "a":
                raise JiraNotFoundError("HTTP-error-code: 404")
            raise JiraForbiddenError("HTTP-error-code: 403")
        self.mock_client.request.side_effect = _request
        self.assertFalse(next(s for s in streams.ALL_STREAMS
                              if s.tap_stream_id == "users").check_access(self.mock_client))

    @mock.patch("tap_jira.streams.Context.config", {"groups": "a"})
    def test_users_missing_group_is_accessible(self):
        self.mock_client.request.side_effect = JiraNotFoundError("HTTP-error-code: 404")
        self.assertTrue(next(s for s in streams.ALL_STREAMS
                             if s.tap_stream_id == "users").check_access(self.mock_client))


def groups_then(member_behaviour, groups_page=None):
    """`client.request` side effect: /group/bulk returns `groups_page`;
    /group/member by `groupId` (the group_users probe) runs
    `member_behaviour(params)`. The `users` stream also reads /group/member,
    but by `groupname`, so it is left accessible."""
    page = {"values": [{"groupId": "g1"}]} if groups_page is None else groups_page

    def _request(tap_stream_id, method, path, **kwargs):
        params = kwargs.get("params") or {}
        if path == "/rest/api/2/group/bulk":
            return page
        if path == "/rest/api/2/group/member" and "groupId" in params:
            return member_behaviour(params)
        return {}
    return _request


def group_id_member_calls(mock_client):
    return [c for c in mock_client.request.call_args_list
            if c.args[2] == "/rest/api/2/group/member"
            and "groupId" in (c.kwargs.get("params") or {})]


def raise_forbidden(_params):
    raise JiraForbiddenError("HTTP-error-code: 403, Error: forbidden")


class TestGroupUsersDiscovery(DiscoveryAccessCheckBase):
    """`group_users` access is validated independently of `groups`."""

    def test_groups_accessible_group_users_forbidden(self):
        self.mock_client.request.side_effect = groups_then(raise_forbidden)
        ids = self.discover_ids()
        self.assertIn("groups", ids)
        self.assertNotIn("group_users", ids)
        # every other stream is untouched
        self.assertEqual(ids, set(ALL_IDS) - {"group_users"})

    def test_member_endpoint_probed_with_valid_group_id(self):
        self.mock_client.request.side_effect = groups_then(lambda _p: {})
        self.assertIn("group_users", self.discover_ids())
        member_calls = group_id_member_calls(self.mock_client)
        self.assertEqual(len(member_calls), 1)
        self.assertEqual(member_calls[0].kwargs["params"]["groupId"], "g1")

    def test_both_accessible(self):
        self.mock_client.request.side_effect = groups_then(lambda _p: {})
        ids = self.discover_ids()
        self.assertIn("groups", ids)
        self.assertIn("group_users", ids)

    def test_empty_groups_keeps_group_users_without_member_probe(self):
        self.mock_client.request.side_effect = groups_then(raise_forbidden, groups_page={"values": []})
        ids = self.discover_ids()
        self.assertIn("groups", ids)
        self.assertIn("group_users", ids)
        self.assertEqual(group_id_member_calls(self.mock_client), [])

    def test_groups_forbidden_excludes_both(self):
        self.mock_client.request.side_effect = forbidden(["/rest/api/2/group/bulk"])
        ids = self.discover_ids()
        self.assertNotIn("groups", ids)
        self.assertNotIn("group_users", ids)

    def test_unrelated_forbidden_stream_does_not_affect_group_users(self):
        group_requests = groups_then(lambda _p: {})

        def _request(tap_stream_id, method, path, **kwargs):
            if "/rest/api/2/role" in path:
                raise JiraForbiddenError("HTTP-error-code: 403, Error: forbidden")
            return group_requests(tap_stream_id, method, path, **kwargs)

        self.mock_client.request.side_effect = _request
        ids = self.discover_ids()
        self.assertNotIn("roles", ids)
        self.assertIn("groups", ids)
        self.assertIn("group_users", ids)

    def test_non_forbidden_error_from_member_endpoint_propagates(self):
        def boom(_params):
            raise RuntimeError("boom")
        self.mock_client.request.side_effect = groups_then(boom)
        with self.assertRaises(RuntimeError):
            tap_jira.discover(self.mock_client)

    def test_on_prem_never_probes_group_endpoints(self):
        self.mock_client.is_on_prem_instance = True
        self.mock_client.request.side_effect = groups_then(raise_forbidden)
        ids = self.discover_ids()
        self.assertNotIn("group_users", ids)
        self.assertEqual(group_id_member_calls(self.mock_client), [])
        paths = [c.args[2] for c in self.mock_client.request.call_args_list]
        self.assertNotIn("/rest/api/2/group/bulk", paths)
