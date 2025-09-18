from unittest import mock
from unittest.mock import Mock
from tap_jira.http import Client
from tap_jira.streams import ALL_STREAMS
from tap_jira.context import Context
import tap_jira
import unittest
import requests
import json

# Mock args
JIRA_CONFIG = {
  "start_date": "2020-02-10",
  "username": "dummy_admin",
  "password": "dummy@000",
  "base_url": "http://127.0.0.1:8000",
}

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

@mock.patch('tap_jira.http.Client.send')
class TestPkSwitchingForUserStream(unittest.TestCase):

    def test_pk_for_cloud_jira(self, mocked_send):
        '''
            Verify is_on_prem_instance property of Client remain False when `deploymentType`
            is `Cloud` in the response
        '''
        mocked_send.return_value = get_mock_http_response(200, {"deploymentType": "Cloud"})

        jira_client = Client(JIRA_CONFIG)

        self.assertEqual(jira_client.is_on_prem_instance, False)

    def test_pk_for_on_prem_jira(self, mocked_send):
        '''
            Verify is_on_prem_instance property of Client changed True when `deploymentType`
            is `Server` in the response
        '''
        mocked_send.return_value = get_mock_http_response(200, {"deploymentType": "Server"})

        jira_client = Client(JIRA_CONFIG)

        self.assertEqual(jira_client.is_on_prem_instance, True)

    @mock.patch('singer.metadata')
    def test_pk_update_for_on_prem_jira(self, mocked_metadata, mocked_send):
        '''
            Verify primary key of users stream is updated to `key` when is_on_prem_instance property
            of Client is True(on prem jira account)
        '''
        mocked_send.return_value = get_mock_http_response(200, {})

        jira_client = Client(JIRA_CONFIG)
        jira_client.is_on_prem_instance = True
        Context.client = jira_client
        # `users` stream
        users_stream = ALL_STREAMS[7]

        # mock schema and it's properties
        schema = Mock()
        schema.properties = {}

        tap_jira.generate_metadata(users_stream, schema)

        # Verify primary key of stream
        self.assertEqual(users_stream.pk_fields, ["key"])

    @mock.patch('singer.metadata')
    def test_pk_update_for_cloud_jira(self, mocked_metadata, mocked_send):
        '''
            Verify primary key of users stream is `accountId` when is_on_prem_instance property
            of Client is False(cloud jira account)
        '''
        mocked_send.return_value = get_mock_http_response(200, {})

        jira_client = Client(JIRA_CONFIG)
        jira_client.is_on_prem_instance = False
        Context.client = jira_client

        # `users` stream
        users_stream = ALL_STREAMS[7]

        # mock schema and it's properties
        schema = Mock()
        schema.properties = {}

        tap_jira.generate_metadata(users_stream, schema)

        # Verify primary key of stream
        self.assertEqual(users_stream.pk_fields, ["accountId"])

    @mock.patch('singer.metadata')
    def test_pk_update_for_non_users_stream(self, mocked_metadata, mocked_send):
        '''
            Verify primary key of all other streams remain unchanged.
        '''
        mocked_send.return_value = get_mock_http_response(200, {})

        jira_client = Client(JIRA_CONFIG)

        # `resolutions` stream(other than users stream)
        users_stream = ALL_STREAMS[5]

        # mock schema and it's properties
        schema = Mock()
        schema.properties = {}

        tap_jira.generate_metadata(users_stream, schema)

        # Verify primary key of stream
        self.assertEqual(users_stream.pk_fields, ["id"])
