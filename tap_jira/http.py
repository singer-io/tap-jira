import requests
import re
from requests.auth import HTTPBasicAuth
from singer import metrics
import backoff
from datetime import datetime, timedelta
import time


class RateLimitException(Exception):
    pass


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")

# The project plan for this tap specified:
# > our past experience has shown that issuing queries no more than once every
# > 10ms can help avoid performance issues
TIME_BETWEEN_REQUESTS = timedelta(microseconds=10e3)

class Client(object):
    def __init__(self, config):
        self.user_agent = config.get("user_agent")
        self.base_url = config["base_url"]
        self.auth = HTTPBasicAuth(config["username"], config["password"])
        self.session = requests.Session()
        self.next_request_at = datetime.now()

    def url(self, path):
        # defend against if the base_url does or does not provide
        # https://
        base_url = self.base_url
        base_url = re.sub('^http[s]?://', '', base_url)
        base_url = 'https://' + base_url
        return _join(base_url, path)

    def _headers(self, headers):
        headers = headers.copy()
        if self.user_agent:
            headers["User-Agent"] = self.user_agent
        return headers

    def send(self, method, path, headers={}, **kwargs):
        request = requests.Request(
            method, self.url(path), auth=self.auth,
            headers=self._headers(headers),
            **kwargs
        )
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
