import unittest
from unittest import mock
import requests
from tap_jira import http
from tap_jira import streams
from tap_jira.context import Context

# mock responce
class Mockresponse:
    def __init__(self, resp, status_code, content=[], headers=None, raise_error=False):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.url = ""
        self.request = mock.Mock()
        self.request.method = ""

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

    def mock_send_409(*args, **kwargs):
        return Mockresponse("",409,raise_error=True)

    def mock_send_429(*args, **kwargs):
        return Mockresponse("",429,raise_error=True)

    def mock_send_449(*args, **kwargs):
        return Mockresponse("",449,raise_error=True)

    def mock_send_500(*args, **kwargs):
        return Mockresponse("",500,raise_error=True)

    def mock_send_501(*args, **kwargs):
        return Mockresponse("",501,raise_error=True)

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
            self.assertEqual(str(e), expected_error_message)


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
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_403)
    def test_request_with_handling_for_403_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: User does not have permission to access the resource."

            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


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
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_409)
    def test_request_with_handling_for_409_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraConflictError as e:
            expected_error_message = "HTTP-error-code: 409, Error: The request does not match our state in some way."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

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
            self.assertEqual(str(e), expected_error_message)
            self.assertEqual(mock_send.call_count,10)

    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_449)
    def test_request_with_handling_for_449_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraSubRequestFailedError as e:
            expected_error_message = "HTTP-error-code: 449, Error: The API was unable to process every part of the request."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_500)
    def test_request_with_handling_for_500_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraInternalServerError as e:
            expected_error_message = "HTTP-error-code: 500, Error: The server encountered an unexpected condition which prevented it from fulfilling the request."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_501)
    def test_request_with_handling_for_501_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraNotImplementedError as e:
            expected_error_message = "HTTP-error-code: 501, Error: The server does not support the functionality required to fulfill the request."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_502)
    def test_request_with_handling_for_502_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraBadGatewayError as e:
            expected_error_message = "HTTP-error-code: 502, Error: Server received an invalid response."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


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
            self.assertEqual(str(e), expected_error_message)
            self.assertEqual(mock_send.call_count,10)

    @mock.patch("tap_jira.http.Client.send",side_effect=mock_send_504)
    def test_request_with_handling_for_504_exceptin_handling(self,mock_send):
        try:
            tap_stream_id = "tap_jira"
            mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
            mock_client = http.Client(mock_config)
            mock_client.request(tap_stream_id)
        except http.JiraGatewayTimeoutError as e:
            expected_error_message = "HTTP-error-code: 504, Error: API service time out, please check Jira server."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


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
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_400_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_400()
            Context.retrieve_timezone()
        except http.JiraBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_401_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_401()
            Context.retrieve_timezone()
        except http.JiraUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_403_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_403()
            Context.retrieve_timezone()
        except http.JiraForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: User does not have permission to access the resource."

            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_404_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_404()
            Context.retrieve_timezone()
        except http.JiraNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_409_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_409()
            Context.retrieve_timezone()
        except http.JiraConflictError as e:
            expected_error_message = "HTTP-error-code: 409, Error: The request does not match our state in some way."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_429_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_429()
            Context.retrieve_timezone()
        except http.JiraRateLimitError as e:
            expected_error_message = "HTTP-error-code: 429, Error: The API rate limit for your organisation/application pairing has been exceeded."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_449_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_449()
            Context.retrieve_timezone()
        except http.JiraSubRequestFailedError as e:
            expected_error_message = "HTTP-error-code: 449, Error: The API was unable to process every part of the request."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_500_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_500()
            Context.retrieve_timezone()
        except http.JiraInternalServerError as e:
            expected_error_message = "HTTP-error-code: 500, Error: The server encountered an unexpected condition which prevented it from fulfilling the request."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_501_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_501()
            Context.retrieve_timezone()
        except http.JiraNotImplementedError as e:
            expected_error_message = "HTTP-error-code: 501, Error: The server does not support the functionality required to fulfill the request."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_502_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_502()
            Context.retrieve_timezone()
        except http.JiraBadGatewayError as e:
            expected_error_message = "HTTP-error-code: 502, Error: Server received an invalid response."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_503_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_503()
            Context.retrieve_timezone()
        except http.JiraServiceUnavailableError as e:
            expected_error_message = "HTTP-error-code: 503, Error: API service is currently unavailable."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_504_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_504()
            Context.retrieve_timezone()
        except http.JiraGatewayTimeoutError as e:
            expected_error_message = "HTTP-error-code: 504, Error: API service time out, please check Jira server."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("tap_jira.context.Context.client")
    def test_retrieve_timezone_with_handling_for_505_exceptin_handling(self,mock_context_client):
        try:
            mock_context_client.send.return_value = self.mock_send_505()
            Context.retrieve_timezone()
        except http.JiraError as e:
            expected_error_message = "HTTP-error-code: 505, Error: Unknown Error"
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

class TestUserGroupSync(unittest.TestCase):
    def mock_raise_404(*args, **kwargs):
        raise http.JiraNotFoundError

    @mock.patch("tap_jira.streams.Paginator.pages", side_effect=mock_raise_404)
    @mock.patch("tap_jira.streams.Context.config")
    @mock.patch("tap_jira.streams.LOGGER.info")
    def test_no_user_group_found(self,mocked_logger, mock_config, mock_raise_404):
        '''
            Verify that if user group is not found then skip message should be print instead of raising exception
        '''
        mock_config.get.return_value = "test"

        user = streams.Users("users", ["accountId"], "INCREMENTAL")
        user.sync()

        # JiraNotFoundError is raised so skipping log should be called
        mocked_logger.assert_called_with('Could not find group "%s", skipping', 'test')

    @mock.patch("tap_jira.streams.Paginator.pages")
    @mock.patch("tap_jira.streams.Context.config")
    @mock.patch("tap_jira.streams.Stream.write_page")
    def test_user_group_found(self,mocked_write_page, mock_config, mock_get_pages):
        '''
            Verify that if user group found then write_page should be called
        '''
        mock_config.get.return_value = "test"
        mock_get_pages.return_value = ["page1", "page2", "page3"] # return 3 mock pages

        user = streams.Users("users", ["accountId"], "INCREMENTAL")
        user.sync()

        # write_page should be called 3 times as three mock pages return
        self.assertEqual(mocked_write_page.call_count, 3)
