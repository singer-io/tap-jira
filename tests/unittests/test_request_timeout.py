import unittest
from unittest import mock
import requests
from tap_jira.http import Client

# Mock response object
def get_mock_http_response(*args, **kwargs):
    contents = '{"access_token": "test", "expires_in":100, "accounts":[{"id": 12}]}'
    response = requests.Response()
    response.status_code = 200
    response._content = contents.encode()
    response.url = ""
    response.request = requests.Request()
    response.request.method = ""
    return response


@mock.patch('requests.Session.send', side_effect = get_mock_http_response)
@mock.patch("requests.Request.prepare")
class TestRequestTimeoutValue(unittest.TestCase):

    def test_no_request_timeout_in_config(self, mocked_prepare, mocked_send):
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        jira_client = Client({"base_url": "https://your-jira-domain"}) # No request_timeout in config
        jira_client.refresh_token = "test"

        # Call request method which call Session.send with timeout
        jira_client.request("test", method="GET", path="http://test")

        # Verify Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument

    def test_integer_request_timeout_in_config(self, mocked_prepare, mocked_send):
        """
            Verify that if request_timeout is provided in config (integer value) then it should be used
        """
        jira_client = Client({
            "base_url": "https://your-jira-domain",
            "request_timeout": 100 # integer timeout in config
            })
        jira_client.refresh_token = "test"

        # Call request method which call Session.send with timeout
        jira_client.request("test", method="GET", path="http://test")

        # Verify Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 100.0) # Verify timeout argument

    def test_float_request_timeout_in_config(self, mocked_prepare, mocked_send):
        """
            Verify that if request_timeout is provided in config (float value) then it should be used
        """
        jira_client = Client({
            "base_url": "https://your-jira-domain",
            "request_timeout": 100.5 # float timeout in config
            })
        jira_client.refresh_token = "test"

        # Call request method which call Session.send with timeout
        jira_client.request("test", method="GET", path="http://test")

        # Verify Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 100.5) # Verify timeout argument

    def test_string_request_timeout_in_config(self, mocked_prepare, mocked_send):
        """
            Verify that if request_timeout is provided in config (string value) then it should be use
        """
        jira_client = Client({
            "base_url": "https://your-jira-domain",
            "request_timeout": "100" # string format timeout in config
            })
        jira_client.refresh_token = "test"

        # Call request method which call Session.send with timeout
        jira_client.request("test", method="GET", path="http://test")

        # Verify requests.request and Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 100) # Verify timeout argument

    def test_empty_string_request_timeout_in_config(self, mocked_prepare, mocked_send):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        jira_client = Client({
            "base_url": "https://your-jira-domain",
            "request_timeout": "" # empty string in config
            })
        jira_client.refresh_token = "test"

        # Call request method which call Session.send with timeout
        jira_client.request("test", method="GET", path="http://test")

        # Verify Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument

    def test_zero_request_timeout_in_config(self, mocked_prepare, mocked_send):
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """
        jira_client = Client({
            "base_url": "https://your-jira-domain",
            "request_timeout": 0.0 # zero value in config
            })
        jira_client.refresh_token = "test"

        # Call request method which call Session.send with timeout
        jira_client.request("test", method="GET", path="http://test")

        # Verify Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument

    def test_zero_string_request_timeout_in_config(self, mocked_prepare, mocked_send):
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        jira_client = Client({
            "base_url": "https://your-jira-domain",
            "request_timeout": '0.0' # zero value in config
            })
        jira_client.refresh_token = "test"

        # Call request method which call Session.send with timeout
        jira_client.request("test", method="GET", path="http://test")

        # Verify Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument



@mock.patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):

    @mock.patch("tap_jira.http.Client.test_basic_credentials_are_authorized")
    @mock.patch('requests.Session.send', side_effect = requests.exceptions.Timeout)
    @mock.patch("requests.Request.prepare")
    def test_request_timeout_backoff(
        self, mocked_prepare, mocked_send, mocked_sleep, mocked_test):
        """
            Verify request function is backoff for 6 times on Timeout exception
        """
        jira_client = Client({"base_url": "https://your-jira-domain"})
        jira_client.refresh_token = "test"

        try:
            jira_client.request("test", method="GET", path="http://test")
        except requests.exceptions.Timeout:
            pass

        # Verify that Session.send is called 6 times
        self.assertEqual(mocked_send.call_count, 6)

    @mock.patch('threading.Timer', return_value = None)
    @mock.patch("tap_jira.http.Client.test_credentials_are_authorized")
    @mock.patch('requests.Session.post', side_effect = requests.exceptions.Timeout)
    def test_timeout_backoff_for_refresh_credentials(
        self, mocked_post, mocked_test, mocked_timer, mocked_sleep):
        """
            Verify refresh_credentials method backoff's for 3 times on Timeout exception
        """
        config = {
            "base_url": "https://your-jira-domain",
            "oauth_client_id": "test",
            "oauth_client_secret": "test",
            "refresh_token": "test"
            }

        try:
            # init Client with `oauth_client_id` in config calls refresh_credentials
            Client(config)
        except Exception:
            pass

        # Verify that Response.raise_for_status is called 3 times
        self.assertEqual(mocked_post.call_count, 3)
