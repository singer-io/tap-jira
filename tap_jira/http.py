import requests
import re
from requests.auth import HTTPBasicAuth
import singer
from singer import metrics
import backoff
from datetime import datetime, timedelta
import time
import threading
from . import streams as streams_


class RateLimitException(Exception):
    pass

# Jira OAuth tokens last for 3600 seconds
REFRESH_TOKEN_EXPIRATION_PERIOD = 3500

# The project plan for this tap specified:
# > our past experience has shown that issuing queries no more than once every
# > 10ms can help avoid performance issues
TIME_BETWEEN_REQUESTS = timedelta(microseconds=10e3)

LOGGER = singer.get_logger()

class Client(object):
    def __init__(self, config):
        self.user_agent = config.get("user_agent")

        self.base_url = 'https://api.atlassian.com/ex/jira/{}{}'

        self.cloud_id = config['cloud_id']
        self.access_token = config['access_token']
        self.refresh_token = config['refresh_token']
        self.oauth_client_id = config['oauth_client_id']
        self.oauth_client_secret = config['oauth_client_secret']

        self.session = requests.Session()
        self.next_request_at = datetime.now()

        self.login_timer = None

    def refresh_credentials(self):
        body = {"grant_type": "refresh_token",
                "client_id": self.oauth_client_id,
                "client_secret": self.oauth_client_secret,
                "refresh_token": self.refresh_token}
        try:
            resp = self.session.post("https://auth.atlassian.com/oauth/token", data=body)
            self.access_token = resp.json()['access_token']
        except Exception as e:
            error_message = str(e)
            if resp:
                error_message = error_message + ", Response from Jira: {}".format(resp.text)
            raise Exception(error_message) from e
        finally:
            LOGGER.info("Starting new login timer")
            self.login_timer = threading.Timer(REFRESH_TOKEN_EXPIRATION_PERIOD, self.refresh_credentials)
            self.login_timer.start()


    def test_credentials_are_authorized(self):
        self.request(streams_.ISSUES.tap_stream_id, "GET", "/rest/api/2/search",
                     params={"maxResults": 1})

    def url(self, path):
        """The base_url for OAuth'd Jira is always the same and uses the provided cloud_id and path"""
        return self.base_url.format(self.cloud_id, path)

    def _headers(self, headers):
        headers = headers.copy()
        if self.user_agent:
            headers["User-Agent"] = self.user_agent

        # Add Accept and Authorization headers
        headers['Accept'] = 'application/json'
        headers['Authorization'] = 'Bearer {}'.format(self.access_token)
        return headers

    def send(self, method, path, headers={}, **kwargs):
        request = requests.Request(method,
                                   self.url(path),
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


class Paginator(object):
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
            page = response[self.items_key]
            if len(page) < response["maxResults"]:
                self.next_page_num = None
            else:
                self.next_page_num += response["maxResults"]
            if page:
                yield page
