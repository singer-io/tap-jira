import functools
import json
import time
import threading
import re
import uuid
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from requests.exceptions import HTTPError
from requests.auth import HTTPBasicAuth
import requests
import backoff

from tap_tester.logger import LOGGER


class RateLimitException(Exception):
    pass

# Jira OAuth tokens last for 3600 seconds. We set it to 3500 to try to
# come in under the limit.
REFRESH_TOKEN_EXPIRATION_PERIOD = 3500

def should_retry_httperror(exception):
    """ Retry 500-range errors. """
    # An ConnectionError is thrown without a response
    if exception.response is None:
        return True

    return 500 <= exception.response.status_code < 600


"""
Almost all the code in this file is copied from the client and stream code in the tap.
This avoids sharing code between the tap and test.
"""
class TestClient():
    def __init__(self, config):
        self.is_cloud = 'oauth_client_id' in config.keys()
        self.session = requests.Session()
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
            self.auth = HTTPBasicAuth(config.get("username"), config.get("password"))

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
        response = self.send(*args, **kwargs)
        if response.status_code == 429:
            raise RateLimitException()

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_error:
            LOGGER.error("Received HTTPError with status code %s, error message response text %s",
                         http_error.response.status_code,
                         http_error.response.text)
            raise

        return response.json()

    def refresh_credentials(self):
        body = {"grant_type": "refresh_token",
                "client_id": self.oauth_client_id,
                "client_secret": self.oauth_client_secret,
                "refresh_token": self.refresh_token}
        try:
            resp = self.session.post("https://auth.atlassian.com/oauth/token", data=body)
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

class TestStream(ABC):
    def __init__(self, client):
        self._client = client

    @abstractmethod
    def create_test_data(self, min_ensure_exists=1):
        pass

    def count_all(self):
        return len(self.get_all())

    def get_first(self):
        return self.get_all()[0]

    @abstractmethod
    def get_all(self):
        pass


class TestProjects(TestStream):
    tap_stream_id = 'projects'

    def create_test_data(self, min_ensure_exists=1):
        raise NotImplementedError("Not implemented yet")

    def get_all(self):
        return self._client.request(
            self.tap_stream_id, "GET", "/rest/api/2/project",
            params={"expand": "description,lead,url,projectKeys"})

class TestUsers(TestStream):
    tap_stream_id = 'users'

    def create_test_data(self, min_ensure_exists=1):
        raise NotImplementedError("Not implemented yet")

    def get_all(self):
        max_results = 2
        groups = [
            "jira-administrators",
            "jira-software-users",
        ]
        all_users = list()
        for group in groups:
            params = {"groupname": group,
                        "maxResults": max_results,
                        "includeInactiveUsers": True}
            pager = Paginator(self._client, items_key='values')
            for page in pager.pages(self.tap_stream_id, "GET",
                                    "/rest/api/2/group/member",
                                    params=params):
                for user in page:
                    all_users.append(user)

        return all_users

class TestComponents(TestStream):
    tap_stream_id = 'components'

    def create_test_data(self, min_ensure_exists=1):
        count = self.count_all()
        LOGGER.info("Found %s records for stream %s", count, self.tap_stream_id)

        if min_ensure_exists - count + 1 > 0:
            LOGGER.info("Need to create %s more records for stream %s, doing so now", min_ensure_exists - count + 1, self.tap_stream_id)
            for _ in range(0, min_ensure_exists - count + 1):
                random_uuid = uuid.uuid4()
                self._client.request(
                    self.tap_stream_id, "POST", "/rest/api/2/component",
                    headers={"Content-Type": "application/json"},
                    data=json.dumps({
                        "isAssigneeTypeValid": False,
                        "name": "Component {}".format(random_uuid),
                        "description": "This is Jira component {}".format(random_uuid),
                        "project": self.__get_test_project()['key'],
                        "leadAccountId": self.__get_test_user()['accountId'],
                    }),
                )

    # Cache means it saves the result of this call forever
    # It assumes project data does not change
    @functools.lru_cache
    def __get_test_project(self):
        return TestProjects(self._client).get_first()

    # Cache means it saves the result of this call forever
    # It assumes user data does not change
    @functools.lru_cache
    def __get_test_user(self):
        return TestUsers(self._client).get_first()

    def get_all(self):
        path = "/rest/api/2/project/{}/component".format(self.__get_test_project()["id"])
        pager = Paginator(self._client)
        all_records = list()
        for page in pager.pages("components", "GET", path):
            for rec in page:
                all_records.append(rec)

        return all_records


ALL_TEST_STREAMS = {
    "components": TestComponents,
    "projects": TestProjects,
    "users": TestUsers,
}

for stream_class in ALL_TEST_STREAMS.values():
    TestStream.register(stream_class)
