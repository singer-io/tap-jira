from datetime import datetime, timedelta
import time
import threading
import re
from requests.exceptions import HTTPError
from requests.auth import HTTPBasicAuth
import requests
from singer import metrics
import singer
import backoff


class RateLimitException(Exception):
    pass


# Jira OAuth tokens last for 3600 seconds. We set it to 3500 to try to
# come in under the limit.
REFRESH_TOKEN_EXPIRATION_PERIOD = 3500

# The project plan for this tap specified:
# > our past experience has shown that issuing queries no more than once every
# > 10ms can help avoid performance issues
TIME_BETWEEN_REQUESTS = timedelta(microseconds=10e3)

LOGGER = singer.get_logger()


def should_retry_httperror(exception):
    """ Retry 500-range errors. """
    # An ConnectionError is thrown without a response
    if exception.response is None:
        return True

    return 500 <= exception.response.status_code < 600


class Client:
    def __init__(self, config):
        self.is_cloud = 'oauth_client_id' in config.keys()
        self.session = requests.Session()
        self.next_request_at = datetime.now()
        self.user_agent = config.get("user_agent")
        self.login_timer = None

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
            self.auth = HTTPBasicAuth(config.get(
                "username"), config.get("password"))

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
                          (requests.exceptions.ConnectionError, HTTPError),
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
        return self.session.send(request.prepare())

    @backoff.on_exception(backoff.constant,
                          RateLimitException,
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
        if response.status_code == 429:
            raise RateLimitException()
        response.raise_for_status()
        return response.json()

    def refresh_credentials(self):
        body = {"grant_type": "refresh_token",
                "client_id": self.oauth_client_id,
                "client_secret": self.oauth_client_secret,
                "refresh_token": self.refresh_token}
        try:
            resp = self.session.post(
                "https://auth.atlassian.com/oauth/token", data=body)
            resp.raise_for_status()
            self.access_token = resp.json()['access_token']
        except Exception as ex:
            error_message = str(ex)
            if resp:
                error_message = error_message + \
                    ", Response from Jira: {}".format(resp.text)
            raise Exception(error_message) from ex
        finally:
            LOGGER.info("Starting new login timer")
            self.login_timer = threading.Timer(REFRESH_TOKEN_EXPIRATION_PERIOD,
                                               self.refresh_credentials)
            self.login_timer.start()

    def test_credentials_are_authorized(self):
        # Assume that everyone has issues, so we try and hit that endpoint
        return self.request("issues", "GET", "/rest/api/2/search",
                            params={"maxResults": 1})

    def get(self, tap_stream_id, endpoint, params=None):
        return self.request(tap_stream_id, "GET",
                            endpoint, params=params)

    def fetch_pages(self, tap_stream_id, endpoint, items_key="values",
                    startAt=0, maxResults=50, orderBy=None, params=None):
        page_params = params.copy() if params else dict()
        if startAt:
            page_params["startAt"] = startAt
        if maxResults:
            page_params["maxResults"] = maxResults
        if orderBy:
            page_params["orderBy"] = orderBy

        with metrics.record_counter(endpoint=endpoint) as counter:
            resource = self.request(tap_stream_id, "GET",
                                    endpoint, params=page_params)

            resource = resource if resource else dict()
            page = resource.get(items_key, [])

            total = resource.get("total")
            start_at_from_response = resource.get("startAt", 0)
            max_results_from_response = resource.get("maxResults", 1)

            cursor = startAt
            page_size = maxResults
            if not maxResults:
                page_size = max_results_from_response or len(page)
                cursor = (startAt or start_at_from_response) + page_size

            counter.increment(len(page))
            yield page, cursor

            while (
                    (total is None or cursor < total)
                    and len(page) == page_size
            ):
                page_params["startAt"] = cursor
                page_params["maxResults"] = page_size
                resource = self.request(
                    tap_stream_id, "GET", endpoint, params=page_params)
                if resource:
                    page = resource.get(items_key)
                    cursor += page_size
                    counter.increment(len(page))
                    yield page, cursor
                else:
                    # if resource is an empty dictionary we assume no-results
                    break
