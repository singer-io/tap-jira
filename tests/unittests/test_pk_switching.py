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


class TestSavedCatalogPrimaryKeys(unittest.TestCase):
    def test_saved_catalog_keys_used_for_schema_and_user_sync(self):
        users_stream = next(stream for stream in ALL_STREAMS if stream.tap_stream_id == "users")
        for pk_field in ("key", "accountId"):
            with self.subTest(pk_field=pk_field):
                catalog = {
                    "streams": [{
                        "stream": "users",
                        "tap_stream_id": "users",
                        "key_properties": [pk_field],
                        "schema": {
                            "type": "object",
                            "properties": {pk_field: {"type": "string"}},
                        },
                        "metadata": [
                            {"breadcrumb": [], "metadata": {
                                "selected": True, "table-key-properties": [pk_field],
                            }},
                            {"breadcrumb": ["properties", pk_field],
                             "metadata": {"inclusion": "automatic"}},
                        ],
                    }],
                }
                args = Mock(config={"groups": "admins,users"}, state={},
                            properties=catalog, discover=False)
                client = Mock(is_on_prem_instance=pk_field == "key", login_timer=None)
                records = [{pk_field: "user1"}, {pk_field: "user2"}]
                with mock.patch.object(users_stream, "pk_fields", ["accountId"]), \
                     mock.patch.multiple(Context, config=None, state=None, catalog=None,
                                         client=None, stream_map={}), \
                     mock.patch("tap_jira.get_args", return_value=args), \
                     mock.patch("tap_jira.Client", return_value=client), \
                     mock.patch("tap_jira.discover") as discover, \
                     mock.patch("tap_jira.streams.Paginator.pages",
                                side_effect=[[records], [[records[0]]]]), \
                     mock.patch("singer.write_schema") as write_schema, \
                     mock.patch("singer.write_state"), \
                     mock.patch("singer.write_record") as write_record:
                    tap_jira.main()

                    discover.assert_not_called()
                    self.assertEqual(users_stream.pk_fields, [pk_field])
                    schema_calls = [
                        call for call in write_schema.call_args_list if call.args[0] == "users"
                    ]
                    self.assertEqual(len(schema_calls), 1)
                    self.assertEqual(schema_calls[0].args[2], [pk_field])
                    self.assertEqual(
                        [call.args[1] for call in write_record.call_args_list], records
                    )
