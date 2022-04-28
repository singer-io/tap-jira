from datetime import datetime, timedelta
import time
import threading
import re
from requests.exceptions import (HTTPError, Timeout)
from requests.auth import HTTPBasicAuth
import requests
from singer import metrics
import singer
import backoff

# Jira OAuth tokens last for 3600 seconds. We set it to 3500 to try to
# come in under the limit.
REFRESH_TOKEN_EXPIRATION_PERIOD = 3500

# The project plan for this tap specified:
# > our past experience has shown that issuing queries no more than once every
# > 10ms can help avoid performance issues
TIME_BETWEEN_REQUESTS = timedelta(microseconds=10e3)

LOGGER = singer.get_logger()

# timeout requests after 300 seconds
REQUEST_TIMEOUT = 300

class JiraError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response

class JiraBackoffError(JiraError):
    pass

class JiraBadRequestError(JiraError):
    pass

class JiraUnauthorizedError(JiraError):
    pass

class JiraForbiddenError(JiraError):
    pass

class JiraSubRequestFailedError(JiraError):
    pass

class JiraBadGatewayError(JiraError):
    pass

class JiraConflictError(JiraError):
    pass

class JiraNotFoundError(JiraError):
    pass

class JiraRateLimitError(JiraBackoffError):
    pass

class JiraServiceUnavailableError(JiraBackoffError):
    pass

class JiraGatewayTimeoutError(JiraError):
    pass

class JiraInternalServerError(JiraError):
    pass

class JiraNotImplementedError(JiraError):
    pass

def should_retry_httperror(exception):
    """ Retry 500-range errors. """
    # An ConnectionError is thrown without a response
    if exception.response is None:
        return True

    return 500 <= exception.response.status_code < 600

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": JiraBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": JiraUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": JiraForbiddenError,
        "message": "User does not have permission to access the resource."
    },
    404: {
        "raise_exception": JiraNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": JiraConflictError,
        "message": "The request does not match our state in some way."
    },
    429: {
        "raise_exception": JiraRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    449:{
        "raise_exception": JiraSubRequestFailedError,
        "message": "The API was unable to process every part of the request."
    },
    500: {
        "raise_exception": JiraInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": JiraNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": JiraBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": JiraServiceUnavailableError,
        "message": "API service is currently unavailable."
    },
    504: {
        "raise_exception": JiraGatewayTimeoutError,
        "message": "API service time out, please check Jira server."
    }
}

def check_status(response):
    # Forming a response message for raising custom exception
    try:
        response_json = response.json()
    except Exception: # pylint: disable=broad-except
        response_json = {}
    if response.status_code != 200:
        message = "HTTP-error-code: {}, Error: {}".format(
            response.status_code,
            response_json.get("errorMessages", [ERROR_CODE_EXCEPTION_MAPPING.get(
                response.status_code, {}).get("message", "Unknown Error")])[0]
        )
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(
            response.status_code, {}).get("raise_exception", JiraError)
        raise exc(message, response) from None

def get_request_timeout(config):
    # Get `request_timeout` value from config
    config_request_timeout = config.get('request_timeout')

    # if config request_timeout is other than 0, "0", or "" then use request_timeout
    if config_request_timeout and float(config_request_timeout):
        request_timeout = float(config_request_timeout)
    else:
        # If value is 0, "0", "", or not passed then it set default to 300 seconds
        request_timeout = REQUEST_TIMEOUT
    return request_timeout

class Client():
    def __init__(self, config):
        self.is_cloud = 'oauth_client_id' in config.keys()
        self.session = requests.Session()
        self.next_request_at = datetime.now()
        self.user_agent = config.get("user_agent")
        self.login_timer = None
        self.timeout = get_request_timeout(config)

        # Assign False for cloud Jira instance
        self.is_on_prem_instance = False

        if self.is_cloud:
            LOGGER.info("Using OAuth based API authentication")
            self.auth = None
            self.base_url = 'https://api.atlassian.com/ex/jira/{}{}'
            self.cloud_id = config.get('cloud_id')
            self.access_token = config.get('access_token')
            self.refresh_token = config.get('refresh_token')
            self.oauth_client_id = config.get('oauth_client_id')
            self.oauth_client_secret = config.get('oauth_client_secret')

            # Only appears to be needed once for any 6 hour period. If
            # running the tap for more than 6 hours is needed this will
            # likely need to be more complicated.
            self.refresh_credentials()
            self.test_credentials_are_authorized()
        else:
            LOGGER.info("Using Basic Auth API authentication")
            self.base_url = config.get("base_url")
            self.auth = HTTPBasicAuth(config.get("username"), config.get("password"))
            self.test_basic_credentials_are_authorized()

    def url(self, path):
        if self.is_cloud:
            return self.base_url.format(self.cloud_id, path)

        # defend against if the base_url does or does not provide https://
        base_url = self.base_url
        base_url = re.sub('^http[s]?://', '', base_url)
        base_url = 'https://' + base_url
        return base_url.rstrip("/") + "/" + path.lstrip("/")

    def _headers(self, headers):
        headers = headers.copy()
        if self.user_agent:
            headers["User-Agent"] = self.user_agent

        if self.is_cloud:
            # Add OAuth Headers
            headers['Accept'] = 'application/json'
            headers['Authorization'] = 'Bearer {}'.format(self.access_token)

        return headers

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError, HTTPError, Timeout),
                          jitter=None,
                          max_tries=6,
                          giveup=lambda e: not should_retry_httperror(e))
    def send(self, method, path, headers={}, **kwargs):
        if self.is_cloud:
            # OAuth Path
            request = requests.Request(method,
                                       self.url(path),
                                       headers=self._headers(headers),
                                       **kwargs)
        else:
            # Basic Auth Path
            request = requests.Request(method,
                                       self.url(path),
                                       auth=self.auth,
                                       headers=self._headers(headers),
                                       **kwargs)
        return self.session.send(request.prepare(), timeout=self.timeout)

    @backoff.on_exception(backoff.constant,
                          JiraBackoffError,
                          max_tries=10,
                          interval=60)
    def request(self, tap_stream_id, *args, **kwargs):
        wait = (self.next_request_at - datetime.now()).total_seconds()
        if wait > 0:
            time.sleep(wait)
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.send(*args, **kwargs)
            self.next_request_at = datetime.now() + TIME_BETWEEN_REQUESTS
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        check_status(response)
        return response.json()

    # backoff for Timeout error is already included in "Exception"
    # as it's a parent class of "Timeout" error
    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def refresh_credentials(self):
        body = {"grant_type": "refresh_token",
                "client_id": self.oauth_client_id,
                "client_secret": self.oauth_client_secret,
                "refresh_token": self.refresh_token}
        try:
            resp = self.session.post(
                "https://auth.atlassian.com/oauth/token",
                data=body,
                timeout=self.timeout)
            resp.raise_for_status()
            self.access_token = resp.json()['access_token']
        except Exception as ex:
            error_message = str(ex)
            if resp:
                error_message = error_message + ", Response from Jira: {}".format(resp.text)
            raise Exception(error_message) from ex
        finally:
            LOGGER.info("Starting new login timer")
            self.login_timer = threading.Timer(REFRESH_TOKEN_EXPIRATION_PERIOD,
                                               self.refresh_credentials)
            self.login_timer.start()

    def test_credentials_are_authorized(self):
        # Assume that everyone has issues, so we try and hit that endpoint
        self.request("issues", "GET", "/rest/api/2/search",
                     params={"maxResults": 1})

    def test_basic_credentials_are_authorized(self):
        # Make a call to myself endpoint for verify creds
        # Here, we are retrieving serverInfo for the Jira instance by which credentials will also be verified.
        # Assign True value to is_on_prem_instance property for on-prem Jira instance
        self.is_on_prem_instance = self.request("users","GET","/rest/api/2/serverInfo").get('deploymentType') == "Server"

class Paginator():
    def __init__(self, client, page_num=0, order_by=None, items_key="values"):
        self.client = client
        self.next_page_num = page_num
        self.order_by = order_by
        self.items_key = items_key

    def pages(self, *args, **kwargs):
        """Returns a generator which yields pages of data. When a given page is
        yielded, the next_page_num property can be used to know what the index
        of the next page is (useful for bookmarking).

        :param args: Passed to Client.request
        :param kwargs: Passed to Client.request
        """
        params = kwargs.pop("params", {}).copy()
        while self.next_page_num is not None:
            params["startAt"] = self.next_page_num
            if self.order_by:
                params["orderBy"] = self.order_by
            response = self.client.request(*args, params=params, **kwargs)
            if self.items_key:
                page = response[self.items_key]
            else:
                page = response

            # Accounts for responses that don't nest their results in a
            # key by falling back to the params `maxResults` setting.
            if 'maxResults' in response:
                max_results = response['maxResults']
            else:
                max_results = params['maxResults']

            if len(page) < max_results:
                self.next_page_num = None
            else:
                self.next_page_num += max_results

            if page:
                yield page
