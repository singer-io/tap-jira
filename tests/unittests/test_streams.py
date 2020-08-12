import unittest
import pytz
from tap_jira import streams, utils, client
from mock import patch
from datetime import datetime


class TestLocalizedRequests(unittest.TestCase):
    def setUp(self):
        self.tzname = 'Europe/Volgograd'

        self.config = {
            'user_agent': 'tap-jira',
            'base_url': 'http://jira.com',
            'username': 'test',
            'password': 'test',
        }

    @patch.object(client.Client, 'fetch_pages')
    def test_issues_local_timezone_in_request(self, cli_mock):
        cli_mock.return_value = []

        cli = client.Client(self.config)

        issues = streams.Issues()

        offset = 1
        start_date = "2017-12-04T19:19:32Z"
        issues.sync(cli, self.config, {}, start_date=start_date, offset=offset)

        user_tz = pytz.timezone(self.tzname)
        expected_start_date = (datetime(2017, 12, 4, 19, 19, tzinfo=pytz.UTC)
                               .astimezone(user_tz)
                               .strftime("%Y-%m-%d %H:%M"))

        params = {"fields": "*all",
                  "expand": "changelog,transitions",
                  "validateQuery": "strict",
                  "jql": "updated >= '{}' order by updated asc".format(expected_start_date)}

        self.assertTrue(cli_mock.called_once_with(
            'issues', 'GET', '/rest/api/2/search', params=params))
