import unittest
import pytz
from tap_jira.context import Context
from unittest.mock import Mock, MagicMock, patch
from tap_jira.streams import Issues, Users
from tap_jira.http import Paginator, IssuesPaginator, JiraNotFoundError
from datetime import datetime

class TestLocalizedRequests(unittest.TestCase):
    def setUp(self):
        self.tzname = 'Europe/Volgograd'
        Context.update_start_date_bookmark = Mock(return_value=datetime(2018,12,12,1,2,3, tzinfo=pytz.UTC))
        Context.retrieve_timezone = Mock(return_value=self.tzname)
        Context.bookmark = Mock()
        Context.set_bookmark = Mock()
        Context.is_selected = Mock()
        IssuesPaginator.pages = Mock(return_value=[])
        Context.client = Mock()

    def test_issues_local_timezone_in_request(self):
        issues = Issues('issues', ['pk_fields'], "INCREMENTAL")
        issues.sync()

        user_tz = pytz.timezone(self.tzname)
        expected_start_date = (datetime(2018, 12, 12, 1, 2, tzinfo=pytz.UTC)
                               .astimezone(user_tz)
                               .strftime("%Y-%m-%d %H:%M"))
        params = {"fields": "*all",
                  "expand": "changelog,transitions",
                  "validateQuery": "strict",
                  "jql": "updated >= '{}' order by updated asc".format(expected_start_date)}
        IssuesPaginator.pages.assert_called_once_with('issues','GET','/rest/api/2/search/jql',params=params)


class TestUsersSync(unittest.TestCase):
    def test_unique_users_across_groups_and_pages(self):
        for pk_field in ("accountId", "key"):
            for custom_groups in (False, True):
                for select_all_fields in (False, True):
                    with self.subTest(pk_field=pk_field, custom_groups=custom_groups,
                                      select_all_fields=select_all_fields):
                        groups = (["admins", "users", "empty"] if custom_groups else
                                  ["jira-administrators", "jira-software-users",
                                   "jira-core-users", "jira-users", "users"])
                        users = [
                            {"accountId": str(i), "key": str(i), "displayName": "User " + str(i)}
                            for i in range(3)
                        ]
                        duplicate = dict(users[0])
                        other_key = "key" if pk_field == "accountId" else "accountId"
                        duplicate[other_key] = "different"
                        members = {
                            groups[0]: [users[0], users[1], duplicate],
                            groups[1]: [duplicate, users[2], users[1]],
                        }
                        requests = []

                        def request(stream_id, method, path, params):
                            requests.append((params["groupname"], params["startAt"]))
                            start = params["startAt"]
                            return {"values": members.get(params["groupname"], [])[start:start + 2]}

                        catalog_entry = Mock()
                        catalog_entry.schema.to_dict.return_value = {
                            "type": "object",
                            "properties": {
                                field: {"type": "string"}
                                for field in ("accountId", "key", "displayName")
                            },
                        }
                        stream_metadata = {
                            ("properties", field): {
                                "inclusion": "automatic" if field == pk_field else "available",
                                "selected": select_all_fields,
                            }
                            for field in ("accountId", "key", "displayName")
                        }
                        stream_metadata[()] = {"selected": True}
                        config = {"groups": " admins , users , empty "} if custom_groups else {}
                        with patch("tap_jira.streams.Context") as context, \
                             patch("tap_jira.streams.metadata.to_map", return_value=stream_metadata), \
                             patch("tap_jira.streams.singer.write_record") as write_record:
                            context.config = config
                            context.client.request.side_effect = request
                            context.get_catalog_entry.return_value = catalog_entry
                            Users("users", [pk_field], "FULL_TABLE").sync()

                        records = [call.args[1] for call in write_record.call_args_list]
                        expected = users if select_all_fields else [
                            {pk_field: user[pk_field]} for user in users
                        ]
                        self.assertEqual(records, expected)
                        self.assertEqual(len(records), len({record[pk_field] for record in records}))
                        self.assertGreater(len(records), 2)
                        self.assertEqual(requests, [
                            (groups[0], 0), (groups[0], 2),
                            (groups[1], 0), (groups[1], 2),
                        ] + [(group, 0) for group in groups[2:]])

    def test_unique_users_are_emitted_again_on_next_sync(self):
        user = {"accountId": "1"}
        with patch("tap_jira.streams.Context") as context, \
             patch("tap_jira.streams.Paginator.pages",
                   side_effect=[JiraNotFoundError("missing"), [[user]], [[user]]] * 2):
            context.config = {"groups": "missing,admins,users"}
            stream = Users("users", ["accountId"], "FULL_TABLE")
            with patch.object(stream, "write_page") as write_page:
                stream.sync()
                stream.sync()
        records = [record for call in write_page.call_args_list for record in call.args[0]]
        self.assertEqual(records, [user, user])
