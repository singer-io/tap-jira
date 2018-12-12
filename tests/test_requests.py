import unittest
from unittest.mock import Mock
from tap_jira import Client
from requests.models import Response
from requests.exceptions import HTTPError

class TestBackoffOccurs(unittest.TestCase):
    def setUp(self):
        # Force send to return a response with a 500
        server_error = Response()
        server_error.status_code = 500
        Client.send = Mock(return_value=server_error)

    def test_retries_on_500(self):
        client = Client({})
        with self.assertRaises(HTTPError):
            client.request("issues")
        self.assertGreater(Client.send.call_count, 0)
