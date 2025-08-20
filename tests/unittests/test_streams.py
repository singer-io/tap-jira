import unittest
import pytz
from tap_jira.context import Context
from unittest.mock import Mock, MagicMock
from tap_jira.streams import Issues
from tap_jira.http import Paginator
from datetime import datetime


class TestLocalizedRequests(unittest.TestCase):
    def setUp(self):
        self.tzname = "Europe/Volgograd"
        Context.update_start_date_bookmark = Mock(
            return_value=datetime(2018, 12, 12, 1, 2, 3, tzinfo=pytz.UTC)
        )
        Context.retrieve_timezone = Mock(return_value=self.tzname)
        Context.bookmark = Mock()
        Context.set_bookmark = Mock()
        Paginator.pages = Mock(return_value=[])

    def test_issues_local_timezone_in_request(self):
        issues = Issues("issues", ["pk_fields"])
        issues.sync()

        user_tz = pytz.timezone(self.tzname)
        expected_start_date = (
            datetime(2018, 12, 12, 1, 2, tzinfo=pytz.UTC)
            .astimezone(user_tz)
            .strftime("%Y-%m-%d %H:%M")
        )
        params = {
            "jql": "updated >= '{}' order by updated asc".format(expected_start_date),
            "fields": ["*all"],
            "expand": "changelog,transitions",
            "maxResults": 50,
        }
        Paginator.pages.assert_called_once_with(
            "issues", "GET", "/rest/api/3/search/jql", params=params
        )

if __name__ == "__main__":
    unittest.main()
