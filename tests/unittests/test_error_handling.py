import unittest
from unittest import mock
import requests
from tap_jira import http

# mock responce
class Mockresponse:
    def __init__(self, resp, status_code, content=[], headers=None, raise_error=False):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error

    def prepare(self):
        return (self.json_data, self.status_code, self.content, self.headers, self.raise_error)

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("mock sample message")

    def json(self):
        return self.text

class TestJiraErrorHandling(unittest.TestCase):

    def mock_send_400(*args, **kwargs):
        return Mockresponse("",400,raise_error=True)

    def mock_send_401(*args, **kwargs):
        return Mockresponse("",401,raise_error=True)

    def mock_send_403(*args, **kwargs):
        return Mockresponse("",403,raise_error=True)

    def mock_send_404(*args, **kwargs):
        return Mockresponse("",404,raise_error=True)

    def mock_send_429(*args, **kwargs):
        return Mockresponse("",429,raise_error=True)

    def mock_send_502(*args, **kwargs):
        return Mockresponse("",502,raise_error=True)

    def mock_send_503(*args, **kwargs):
        return Mockresponse("",503,raise_error=True)

    def mock_send_504(*args, **kwargs):
        return Mockresponse("",504,raise_error=True)

    def mock_send_505(*args, **kwargs):
        return Mockresponse("",505,raise_error=True)    

    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_400)
    def test_request_with_handling_for_400_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            

    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_401)
    def test_request_with_handling_for_401_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            

    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_403)
    def test_request_with_handling_for_403_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: User doesn't have permission to access the resource."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            

    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_404)
    def test_request_with_handling_for_404_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_send.call_count,10)

    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_429)
    def test_request_with_handling_for_429_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraRateLimitError as e:
            expected_error_message = "HTTP-error-code: 429, Error: The API rate limit for your organisation/application pairing has been exceeded."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_send.call_count,10)

    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_502)
    def test_request_with_handling_for_502_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraBadGateway as e:
            expected_error_message = "HTTP-error-code: 502, Error: Server received an invalid response."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            

    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_503)
    def test_request_with_handling_for_503_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraServiceUnavailableError as e:
            expected_error_message = "HTTP-error-code: 503, Error: API service is currently unavailable."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_send.call_count,10)

    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_504)
    def test_request_with_handling_for_504_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraGatewayTimeout as e:
            expected_error_message = "HTTP-error-code: 504, Error: API service time out, please check Jira server."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            

    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_505)
    def test_request_with_handling_for_505_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraError as e:
            expected_error_message = "HTTP-error-code: 505, Error: Unknown Error"
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            