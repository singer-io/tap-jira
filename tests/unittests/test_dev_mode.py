import json
import os
import unittest
from unittest.mock import patch, MagicMock

import singer

from tap_jira.http import Client


LOGGER = singer.get_logger()


class TestClientDevMode(unittest.TestCase):
    """Test the dev mode functionality."""

    def setUp(self):
        """Creates a sample config for test execution"""
        # Data to be written
        self.mock_config = {
            "oauth_client_secret": "sample_client_secret",
            "user_agent": "test_user_agent",
            "oauth_client_id": "sample_client_id",
            "access_token": "sample_access_token",
            "cloud_id": "1234567890",
            "refresh_token": "sample_refresh_token",
            "start_date": "2017-12-04T19:19:32Z",
            "request_timeout": 300,
            "groups": "jira-administrators, site-admins, jira-software-users",
        }

        self.tmp_config_filename = "sample_jira_config.json"

        # Serializing json
        json_object = json.dumps(self.mock_config, indent=4)
        # Writing to sample_quickbooks_config.json
        with open(self.tmp_config_filename, "w") as outfile:
            outfile.write(json_object)

    def tearDown(self):
        """Deletes the sample config"""
        if os.path.isfile(self.tmp_config_filename):
            os.remove(self.tmp_config_filename)

    @patch("tap_jira.http.Client.request", return_value=MagicMock(status_code=200))
    @patch("requests.Session.post", return_value=MagicMock(status_code=200))
    @patch("tap_jira.http.Client._write_config")
    def test_client_with_dev_mode(
        self, mock_write_config, mock_post_request, mock_request
    ):
        """Checks the dev mode implementation and verifies write config functionality is
        not called"""
        Client(
            config_path=self.tmp_config_filename, config=self.mock_config, dev_mode=True
        )

        # _write_config function should never be called as it will update the config
        self.assertEqual(mock_write_config.call_count, 0)

    @patch("tap_jira.http.Client.request", return_value=MagicMock(status_code=200))
    @patch("requests.Session.post", side_effect=Exception())
    def test_client_dev_mode_missing_access_token(
        self, mock_post_request, mock_request
    ):
        """Exception should be raised if missing access token"""

        del self.mock_config["access_token"]

        with self.assertRaises(Exception):
            Client(
                config_path=self.tmp_config_filename,
                config=self.mock_config,
                dev_mode=True,
            )
