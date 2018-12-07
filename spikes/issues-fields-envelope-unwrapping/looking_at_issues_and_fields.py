#!/usr/bin/env python3
import requests
import threading
import time
from datetime import datetime
from datetime import timedelta
from singer import utils

# Jira OAuth tokens last for 3600 seconds
REFRESH_TOKEN_EXPIRATION_PERIOD = 3500

# The project plan for this tap specified:
# > our past experience has shown that issuing queries no more than once every
# > 10ms can help avoid performance issues
TIME_BETWEEN_REQUESTS = timedelta(microseconds=10e3)

REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]


class Client():

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

            self.login_timer = threading.Timer(REFRESH_TOKEN_EXPIRATION_PERIOD, self.refresh_credentials)
            self.login_timer.start()


    def test_credentials_are_authorized(self):
        self.request('issues', "GET", "/rest/api/2/search",
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


    def request(self, tap_stream_id, *args, **kwargs):
        wait = (self.next_request_at - datetime.now()).total_seconds()
        if wait > 0:
            time.sleep(wait)

        response = self.send(*args, **kwargs)
        self.next_request_at = datetime.now() + TIME_BETWEEN_REQUESTS

        if response.status_code == 429:
            raise RateLimitException()
        response.raise_for_status()
        return response.json()


class Context(object):
    config = None
    state = None
    client = None


def establish_client():
    Context.client = Client(Context.config)
    Context.client.refresh_credentials()
    Context.client.test_credentials_are_authorized()


def setup_context():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    Context.config = args.config
    Context.state = args.state


def spike_on(fields, issues):
    # Given the responses to API calls to the `fields` and `issues`
    # endpoints, where the `fields` call was specifically for `issue-id==
    # GEN122DCEA-1`, determine if it is feasable to:
    #
    # 1. Remove the `fields` field in `issues` objects
    # 2. Add the data from (1) to the top-level fields of the `issues`
    #    object
    # 3. Swap the values of certain fields of the `issues` objects using
    #    the mapping from the `fields` endpoint

    # The following should be output to a file in './tap_files' and to
    # STDOUT
    #
    # TODO: A raw `issues` object
    # TODO: A raw `fields` object
    # TODO: Proof that bubbling up fields from `fields` leads to name
    #       collisions
    # TODO: Proof that "name sanitizing" is a complex task
    # TODO: Minimize the amount of tampering with the data
    
    fields_schemas = []
    fields_names = []
    fields_ids = []
    droppable_fields_fields = set()
    problem_resp_items = []

    for field in fields:
        # Get fields of interest
        try:
            fields_schemas.append(field['schema'])
        except KeyError:
            fields_schemas.append('NO_SCHEMA_FOUND')
            problem_resp_items.append(field)
            continue
        fields_names.append(field.get('name','NO_NAME_FOUND'))
        fields_ids.append(field.get('id','NO_ID_FOUND'))

        # Save all other fields
        for trash in field.keys():
            if trash not in ['id','name','schema']:
                droppable_fields_fields.add(trash)

        issues_keys = list(issues.keys())
        issues_fields_keys = list(issues['fields'].keys())

    
def main():
    
    setup_context()
    
    try:
        establish_client()

        resp = Context.client.request("fields", "GET", "/rest/api/3/field")
        issue_resp = Context.client.request("issues", "GET", "/rest/api/3/issue/GEN122DCEA-1")

        spike_on(resp,issue_resp)

    finally:
        if Context.client and Context.client.login_timer:
            Context.client.login_timer.cancel()
        print('Spike Completed!')


if __name__ == '__main__':
    main()
