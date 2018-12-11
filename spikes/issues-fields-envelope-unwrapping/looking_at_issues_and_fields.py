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


def write_header(title):
    print('\n\n#----------------------------------------#')
    print(title)
    print('#----------------------------------------#')

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

    fields_schemas = []
    fields_names = [field.get('name','NO_NAME_FOUND') for field in fields]
    fields_ids = [field.get('id','NO_ID_FOUND') for field in fields]

    fields_id_to_name = {f.get('id','NOT_FOUND'):f.get('name','NOT_FOUND') for f in fields}
    processed_issue_nested_fields = {fields_id_to_name[k]:v for k,v in issues['fields'].items()}

    problem_resp_items = []
    issue_fields = list(issues.keys())
    issue_nested_fields = list(issues['fields'].keys())

    for field in fields:
        # Get fields of interest
        try:
            fields_schemas.append(field['schema'])
        except KeyError:
            fields_schemas.append('NO_SCHEMA_FOUND')
            problem_resp_items.append(field)
            continue

    write_header("Here is a raw `issues` object")
    print(issues)

    write_header("Here are some raw `fields` object: one standard, one custom")
    print(fields[0])
    print(fields[1])

    write_header("Here are the top level keys from `issues`")
    print(issue_fields)

    write_header("Here are the keys from the `issues` envelope")
    print(issue_nested_fields)

    write_header("The following are the keys found in `issues`'s top level fields and envelope")
    found_in_both = [f for f in processed_issue_nested_fields if f in issue_fields]
    print(found_in_both)

    write_header('Here are strings we would need to sanitize (`fields`["name"])')
    print(fields_names)

    write_header('Here are the values of the `schema` key of `fields` objects')
    print(fields_schemas)


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


if __name__ == '__main__':
    main()
