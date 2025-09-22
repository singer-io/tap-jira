from unittest import mock

from requests.exceptions import HTTPError
import tap_jira
import unittest
import requests
import json

# Mock args
class Args():
    def __init__(self):
        self.discover = True
        self.properties = False
        self.config = {}
        self.state = False

# Mock response
def get_mock_http_response(status_code, content={}):
    contents = json.dumps(content)
    response = requests.Response()
    response.status_code = status_code
    response.headers = {}
    response._content = contents.encode()
    response.url = ""
    response.request = requests.Request()
    response.request.method = ""
    return response

@mock.patch('tap_jira.get_args')
@mock.patch('tap_jira.http.Client.send')
@mock.patch('tap_jira.discover')
class TestBasicAuthInDiscoverMode(unittest.TestCase):

    def test_basic_auth_no_access_401(self, mocked_discover, mocked_send, mocked_args):
        '''
            Verify exception is raised for no access(401) error code for basic auth
            and discover is not called.
        '''
        mocked_send.return_value = get_mock_http_response(401, {})
        mocked_args.return_value = Args()
        try:
            tap_jira.main()
        except tap_jira.http.JiraUnauthorizedError as e:
            self.assertEqual(e.response.status_code, 401)
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        self.assertEqual(mocked_discover.call_count, 0)

    def test_basic_auth_access_200(self, mocked_discover, mocked_send, mocked_args):
        '''
            Verify discover mode is called if basic auth credentials are valid
            and dicover function is called twice for setup Context and dicover mode.
        '''
        mocked_send.return_value = get_mock_http_response(200, {})
        mocked_args.return_value = Args()
        tap_jira.main()
        self.assertEqual(mocked_discover.call_count, 2)
