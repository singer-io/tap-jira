import unittest
from types import SimpleNamespace
from unittest import mock

import tap_jira
from tap_jira import streams
from tap_jira.http import JiraForbiddenError, JiraNotFoundError


def make_forbidden():
    return JiraForbiddenError("HTTP-error-code: 403, Error: forbidden")


def fake_stream(stream_id, side_effect=None, indirect=False):
    return SimpleNamespace(tap_stream_id=stream_id,
                           indirect_stream=indirect,
                           sync=mock.Mock(side_effect=side_effect))


@mock.patch("tap_jira.singer.write_state")
@mock.patch("tap_jira.output_schema")
@mock.patch("tap_jira.streams_.validate_dependencies")
@mock.patch("tap_jira.Context.is_selected", return_value=True)
class TestMainSyncLoop(unittest.TestCase):
    """A 403 on one stream must not stop the other streams from syncing."""

    def setUp(self):
        patcher = mock.patch("tap_jira.Context.state", {})
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_sync(self, fake_streams):
        with mock.patch("tap_jira.streams_.ALL_STREAMS", fake_streams):
            tap_jira.sync()

    def test_forbidden_stream_is_skipped_and_others_continue(self, *_mocks):
        first = fake_stream("first")
        forbidden_stream = fake_stream("forbidden_one", side_effect=make_forbidden())
        last = fake_stream("last")

        self.run_sync([first, forbidden_stream, last])

        first.sync.assert_called_once()
        forbidden_stream.sync.assert_called_once()
        last.sync.assert_called_once()

    def test_forbidden_stream_is_logged(self, *_mocks):
        with mock.patch("tap_jira.LOGGER") as mock_logger:
            self.run_sync([fake_stream("forbidden_one", side_effect=make_forbidden())])
        self.assertTrue(mock_logger.warning.called)
        self.assertIn("forbidden_one", mock_logger.warning.call_args.args)

    def test_state_is_cleared_after_a_forbidden_stream(self, *_mocks):
        self.run_sync([fake_stream("forbidden_one", side_effect=make_forbidden())])
        self.assertIsNone(tap_jira.Context.state["currently_syncing"])

    def test_indirect_streams_are_still_not_synced_directly(self, *_mocks):
        indirect = fake_stream("child", indirect=True)
        self.run_sync([indirect])
        indirect.sync.assert_not_called()

    def test_other_errors_still_fail_the_run(self, *_mocks):
        """Only JiraForbiddenError is tolerated; nothing else is suppressed."""
        broken = fake_stream("broken", side_effect=RuntimeError("boom"))
        later = fake_stream("later")
        with self.assertRaises(RuntimeError):
            self.run_sync([broken, later])
        later.sync.assert_not_called()

    def test_not_found_is_not_suppressed(self, *_mocks):
        broken = fake_stream("broken", side_effect=JiraNotFoundError("HTTP-error-code: 404"))
        with self.assertRaises(JiraNotFoundError):
            self.run_sync([broken])


GROUPS_PAGE_1 = [{"groupId": "g1"}, {"groupId": "g2"}]
GROUPS_PAGE_2 = [{"groupId": "g3"}]


def new_groups_stream():
    return streams.Groups("groups", ["groupId"], "FULL_TABLE", path="/rest/api/2/group/bulk")


@mock.patch("tap_jira.streams.Context.client")
@mock.patch("tap_jira.streams.Stream.write_page")
class TestGroupsSyncForbidden(unittest.TestCase):

    def test_groups_forbidden_propagates_to_the_sync_loop(self, mock_write_page, mock_client):
        """`groups` itself being forbidden is handled by the main sync loop."""
        mock_client.is_on_prem_instance = False
        mock_client.request.side_effect = make_forbidden()

        with self.assertRaises(JiraForbiddenError):
            new_groups_stream().sync()
        mock_write_page.assert_not_called()

    @mock.patch("tap_jira.streams.Context.is_selected", return_value=True)
    @mock.patch("tap_jira.streams.Paginator.pages")
    def test_group_users_forbidden_does_not_stop_groups(
            self, mock_pages, mock_is_selected, mock_write_page, mock_client):
        mock_client.is_on_prem_instance = False
        mock_pages.return_value = [GROUPS_PAGE_1, GROUPS_PAGE_2]

        groups = new_groups_stream()
        with mock.patch.object(groups, "sync_group_users",
                               side_effect=make_forbidden()) as mock_group_users:
            groups.sync()  # must not raise

        # Every page of groups is still written, including those after the 403.
        mock_write_page.assert_has_calls([mock.call(GROUPS_PAGE_1), mock.call(GROUPS_PAGE_2)])
        self.assertEqual(mock_write_page.call_count, 2)
        # Once /group/member is known to be forbidden it isn't requested again.
        mock_group_users.assert_called_once_with("g1")

    @mock.patch("tap_jira.streams.Context.is_selected", return_value=True)
    def test_group_users_forbidden_midway_keeps_earlier_data_and_later_groups(
            self, mock_is_selected, mock_write_page, mock_client):
        """Real Paginator: 403 on the second group's members only."""
        mock_client.is_on_prem_instance = False

        def request(tap_stream_id, method, path, params=None, **kwargs):
            if path == "/rest/api/2/group/bulk":
                return {"values": GROUPS_PAGE_1, "maxResults": 50, "isLast": True}
            if params["groupId"] == "g1":
                return {"values": [{"accountId": "a1"}], "maxResults": 50, "isLast": True}
            raise make_forbidden()

        mock_client.request.side_effect = request

        new_groups_stream().sync()

        written = [c.args[0] for c in mock_write_page.call_args_list]
        # groups page, then g1's members; g2's members were forbidden
        self.assertEqual(written, [GROUPS_PAGE_1, [{"accountId": "a1", "groupId": "g1"}]])

    @mock.patch("tap_jira.streams.Context.is_selected", return_value=True)
    @mock.patch("tap_jira.streams.Paginator.pages")
    def test_group_users_forbidden_is_logged(
            self, mock_pages, mock_is_selected, mock_write_page, mock_client):
        mock_client.is_on_prem_instance = False
        mock_pages.return_value = [GROUPS_PAGE_1]
        groups = new_groups_stream()

        with mock.patch.object(groups, "sync_group_users", side_effect=make_forbidden()), \
             mock.patch("tap_jira.streams.LOGGER") as mock_logger:
            groups.sync()

        self.assertTrue(mock_logger.warning.called)
        self.assertIn("group_users", mock_logger.warning.call_args.args)

    @mock.patch("tap_jira.streams.Context.is_selected", return_value=True)
    @mock.patch("tap_jira.streams.Paginator.pages")
    def test_other_group_users_errors_are_not_suppressed(
            self, mock_pages, mock_is_selected, mock_write_page, mock_client):
        mock_client.is_on_prem_instance = False
        mock_pages.return_value = [GROUPS_PAGE_1]
        groups = new_groups_stream()

        with mock.patch.object(groups, "sync_group_users", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                groups.sync()

    @mock.patch("tap_jira.streams.Context.is_selected", return_value=True)
    @mock.patch("tap_jira.streams.Paginator.pages")
    def test_group_users_accessible_syncs_every_group(
            self, mock_pages, mock_is_selected, mock_write_page, mock_client):
        """Existing behaviour is unchanged when nothing is forbidden."""
        mock_client.is_on_prem_instance = False
        mock_pages.return_value = [GROUPS_PAGE_1, GROUPS_PAGE_2]
        groups = new_groups_stream()

        with mock.patch.object(groups, "sync_group_users") as mock_group_users:
            groups.sync()

        self.assertEqual([c.args[0] for c in mock_group_users.call_args_list], ["g1", "g2", "g3"])


@mock.patch("tap_jira.streams.Context.client")
class TestSyncGroupUsersPropagates(unittest.TestCase):
    def test_sync_group_users_does_not_swallow_forbidden(self, mock_client):
        """`sync_group_users` raises; `Groups.sync` decides how to handle it."""
        mock_client.request.side_effect = make_forbidden()
        with self.assertRaises(JiraForbiddenError):
            new_groups_stream().sync_group_users("g1")
