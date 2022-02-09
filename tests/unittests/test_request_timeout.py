import unittest
import requests
from tap_jira.http import Client
from unittest import mock

# Mock response object
def get_mock_http_response(*args, **kwargs):
    contents = '{"access_token": "test", "expires_in":100, "accounts":[{"id": 12}]}'
    response = requests.Response()
    response.status_code = 200
    response._content = contents.encode()
    return response

@mock.patch('requests.Session.send', side_effect = get_mock_http_response)
@mock.patch('tap_jira.http.Client.test_basic_credentials_are_authorized')
class TestRequestTimeoutValue(unittest.TestCase):

    def test_no_request_timeout_in_config(self, mocked_test_cred, mocked_request):
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        config = {"base_url": "test"} # No request_timeout in config
        client = Client(config) # No request_timeout in config

        # Call _make_request method which call Session.request with timeout
        client.send("GET", "/test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument

    def test_integer_request_timeout_in_config(self, mocked_test_cred, mocked_request):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be used.
        """
        config = {"base_url": "test", "request_timeout": 100} # integer timeout in config
        client = Client(config)

        # Call _make_request method which call Session.request with timeout
        client.send("GET", "/test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 100.0) # Verify timeout argument

    def test_float_request_timeout_in_config(self, mocked_test_cred, mocked_request):
        """
            Verify that if request_timeout is provided in config(float value) then it should be used.
        """
        config = {"base_url": "test", "request_timeout": 100.5} #float timeout in config
        client = Client(config)

        # Call _make_request method which call Session.request with timeout
        client.send("GET", "/test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 100.5) # Verify timeout argument

    def test_string_request_timeout_in_config(self, mocked_test_cred, mocked_request):
        """
            Verify that if request_timeout is provided in config(string value) then it should be used.
        """
        config = {"base_url": "test", "request_timeout": "100"} # string format timeout in config
        client = Client(config)

        # Call _make_request method which call Session.request with timeout
        client.send("GET", "/test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 100) # Verify timeout argument

    def test_empty_string_request_timeout_in_config(self, mocked_test_cred, mocked_request):
        """
            Verify that if request_timeout is provided in the config with an empty string then the default value got used.
        """
        config = {"base_url": "test", "request_timeout": ""} # empty string in config
        client = Client(config)

        # Call _make_request method which call Session.request with timeout
        client.send("GET", "/test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument

    def test_zero_request_timeout_in_config(self, mocked_test_cred, mocked_request):
        """
            Verify that if request_timeout is provided in the config with zero value then the default value got used.
        """
        config = {"base_url": "test", "request_timeout": 0.0} # zero value in config
        client = Client(config)

        # Call _make_request method which call Session.request with timeout
        client.send("GET", "/test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument

    def test_zero_string_request_timeout_in_config(self, mocked_test_cred, mocked_request):
        """
            Verify that if request_timeout is provided in the config with zero in string format then the default value got used.
        """
        config = {"base_url": "test", "request_timeout": '0.0'} # zero value in config
        client = Client(config)

        # Call _make_request method which call Session.request with timeout
        client.send("GET", "/test")

        # Verify session.request is called with expected timeout
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument


@mock.patch("time.sleep")
@mock.patch('tap_jira.http.Client.test_basic_credentials_are_authorized')
class TestRequestTimeoutBackoff(unittest.TestCase):

    @mock.patch('requests.Session.send', side_effect = requests.exceptions.Timeout)
    def test_request_timeout_backoff(self, mocked_request, mocked_test_creds, mocked_sleep):
        """
            Verify request function is backing off 6 times on the Timeout exception.
        """
        # Initialize client object
        config = {"base_url": "test"}
        client = Client(config)

        with self.assertRaises(requests.exceptions.Timeout):
            client.send("GET", "/test")

        # Verify that Session.send is called 6 times
        self.assertEqual(mocked_request.call_count, 6)
