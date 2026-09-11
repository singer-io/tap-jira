import unittest
from unittest import mock
from tap_jira import streams


class TestGroupsSync(unittest.TestCase):
    '''
        Verify the `groups` stream calls the `/rest/api/2/group/bulk` endpoint
        through the Paginator and writes every page it receives.
    '''

    @mock.patch("tap_jira.streams.Context.client")
    @mock.patch("tap_jira.streams.Context.is_selected", return_value=False)
    @mock.patch("tap_jira.streams.Stream.write_page")
    @mock.patch("tap_jira.streams.Paginator.pages")
    def test_groups_sync_calls_paginator_with_expected_endpoint(self, mock_pages, mock_write_page, mock_is_selected, mock_client):
        mock_client.is_on_prem_instance = False
        mock_pages.return_value = []

        groups = streams.Groups("groups", ["groupId"], "FULL_TABLE", path="/rest/api/2/group/bulk")
        groups.sync()

        mock_pages.assert_called_with("groups", "GET", "/rest/api/2/group/bulk")
        mock_write_page.assert_not_called()

    @mock.patch("tap_jira.streams.Context.client")
    @mock.patch("tap_jira.streams.Context.is_selected", return_value=False)
    @mock.patch("tap_jira.streams.Stream.write_page")
    @mock.patch("tap_jira.streams.Paginator.pages")
    def test_groups_sync_writes_every_page(self, mock_pages, mock_write_page, mock_is_selected, mock_client):
        mock_client.is_on_prem_instance = False
        mock_pages.return_value = [["group1", "group2"], ["group3"]]

        groups = streams.Groups("groups", ["groupId"], "FULL_TABLE", path="/rest/api/2/group/bulk")
        groups.sync()

        # write_page should be called once per page returned by the paginator
        self.assertEqual(mock_write_page.call_count, 2)
        mock_write_page.assert_has_calls([
            mock.call(["group1", "group2"]),
            mock.call(["group3"]),
        ])

    @mock.patch("tap_jira.streams.Context.is_selected", return_value=False)
    @mock.patch("tap_jira.streams.Paginator", wraps=streams.Paginator)
    @mock.patch("tap_jira.streams.Stream.write_page")
    @mock.patch("tap_jira.streams.Context.client")
    def test_groups_sync_paginator_uses_values_items_key(self, mock_client, mock_write_page, mock_paginator_cls, mock_is_selected):
        mock_client.is_on_prem_instance = False
        # First page is short (less than default maxResults) so pagination stops after one call
        mock_client.request.return_value = {
            "isLast": True,
            "maxResults": 10,
            "startAt": 0,
            "total": 2,
            "values": [
                {"groupId": "id-1", "name": "jdog-developers"},
                {"groupId": "id-2", "name": "juvenal-bot"},
            ],
        }

        groups = streams.Groups("groups", ["groupId"], "FULL_TABLE", path="/rest/api/2/group/bulk")
        groups.sync()

        mock_paginator_cls.assert_called_with(mock_client, items_key="values")
        mock_write_page.assert_called_once_with([
            {"groupId": "id-1", "name": "jdog-developers"},
            {"groupId": "id-2", "name": "juvenal-bot"},
        ])


class TestGroupsSyncGroupUsers(unittest.TestCase):
    '''
        Verify `group_users` is synced as a child of `groups`: only when
        `group_users` is selected, and only via the `groups` stream's sync.
    '''

    @mock.patch("tap_jira.streams.Context.client")
    @mock.patch("tap_jira.streams.Context.is_selected", return_value=False)
    @mock.patch("tap_jira.streams.Stream.write_page")
    @mock.patch("tap_jira.streams.Paginator.pages")
    def test_group_users_not_synced_when_not_selected(self, mock_pages, mock_write_page, mock_is_selected, mock_client):
        mock_client.is_on_prem_instance = False
        mock_pages.return_value = [[{"groupId": "id-1", "name": "jdog-developers"}]]

        groups = streams.Groups("groups", ["groupId"], "FULL_TABLE", path="/rest/api/2/group/bulk")
        groups.sync()

        # Only the `groups` page endpoint should be requested, never group/member
        mock_pages.assert_called_once_with("groups", "GET", "/rest/api/2/group/bulk")

    @mock.patch("tap_jira.streams.Context.client")
    @mock.patch("tap_jira.streams.Context.is_selected", return_value=True)
    @mock.patch("tap_jira.streams.Stream.write_page")
    @mock.patch("tap_jira.streams.Paginator.pages")
    def test_group_users_synced_for_each_group_when_selected(self, mock_pages, mock_write_page, mock_is_selected, mock_client):
        mock_client.is_on_prem_instance = False
        mock_pages.side_effect = [
            [[{"groupId": "id-1", "name": "jdog-developers"},
              {"groupId": "id-2", "name": "juvenal-bot"}]],
            [],  # members for id-1
            [],  # members for id-2
        ]

        groups = streams.Groups("groups", ["groupId"], "FULL_TABLE", path="/rest/api/2/group/bulk")
        groups.sync()

        mock_pages.assert_any_call("group_users", "GET", "/rest/api/2/group/member",
                                   params={"groupId": "id-1", "includeInactiveUsers": True})
        mock_pages.assert_any_call("group_users", "GET", "/rest/api/2/group/member",
                                   params={"groupId": "id-2", "includeInactiveUsers": True})

    @mock.patch("tap_jira.streams.Stream.write_page")
    @mock.patch("tap_jira.streams.Context.is_selected", return_value=True)
    def test_sync_group_users_injects_group_id_and_writes_page(self, mock_is_selected, mock_write_page):
        with mock.patch("tap_jira.streams.Paginator.pages") as mock_pages:
            mock_pages.return_value = [
                [{"accountId": "acc-1"}, {"accountId": "acc-2"}],
            ]

            groups = streams.Groups("groups", ["groupId"], "FULL_TABLE", path="/rest/api/2/group/bulk")
            groups.sync_group_users("id-1")

            mock_write_page.assert_called_once_with([
                {"accountId": "acc-1", "groupId": "id-1"},
                {"accountId": "acc-2", "groupId": "id-1"},
            ])


class TestGroupsSyncOnPrem(unittest.TestCase):
    '''
        Verify the `groups` stream is skipped (rather than failing) when run
        against an on-prem instance, since /rest/api/2/group/bulk is a
        Cloud-only endpoint with no on-prem equivalent.
    '''

    @mock.patch("tap_jira.streams.Context.client")
    @mock.patch("tap_jira.streams.Stream.write_page")
    @mock.patch("tap_jira.streams.Paginator.pages")
    def test_groups_sync_skips_and_does_not_request_on_prem(self, mock_pages, mock_write_page, mock_client):
        mock_client.is_on_prem_instance = True

        groups = streams.Groups("groups", ["groupId"], "FULL_TABLE", path="/rest/api/2/group/bulk")
        groups.sync()

        mock_pages.assert_not_called()
        mock_write_page.assert_not_called()


class TestGroupsStreamRegistration(unittest.TestCase):
    '''Verify the `groups` stream is registered correctly in ALL_STREAMS.'''

    def test_groups_stream_present_in_all_streams(self):
        groups_streams = [s for s in streams.ALL_STREAMS if s.tap_stream_id == "groups"]
        self.assertEqual(len(groups_streams), 1)

        groups_stream = groups_streams[0]
        self.assertIsInstance(groups_stream, streams.Groups)
        self.assertEqual(groups_stream.pk_fields, ["groupId"])
        self.assertEqual(groups_stream.forced_replication_method, "FULL_TABLE")
        self.assertEqual(groups_stream.path, "/rest/api/2/group/bulk")
        self.assertFalse(groups_stream.indirect_stream)
        self.assertTrue(groups_stream.cloud_only)
