from datetime import datetime, timedelta, timezone
import time
import threading
import re
from requests.exceptions import HTTPError
from requests.auth import HTTPBasicAuth
import requests
from requests.adapters import HTTPAdapter
import atlassian_jwt
from singer import metrics
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


def should_retry_httperror(exception):
    """ Retry 500-range errors. """
    # An ConnectionError is thrown without a response
    if exception.response is None:
        return True

    return 500 <= exception.response.status_code < 600


class Client():
    def __init__(self, config, logger):
        self.logger = logger
        self.is_cloud = 'oauth_client_id' in config.keys()
        self.jwt_client_key = config.get('jwt_client_key')
        self.jwt_shared_secret = config.get('jwt_shared_secret')
        self.session = requests.Session()
        
        # Configure connection pool for parallel bulk fetching
        # Increase pool_connections and pool_maxsize to handle concurrent requests within each project
        adapter = HTTPAdapter(
            pool_connections=20,  # Number of urllib3 connection pools to cache
            pool_maxsize=20,      # Maximum number of connections to save in the pool
            max_retries=0         # We handle retries manually with Retry-After headers
        )
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        
        self.next_request_at = datetime.now()
        self.user_agent = config.get("user_agent")
        self.login_timer = None

        if self.is_cloud:
            self.logger.info("Using OAuth based API authentication")
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
        elif self.jwt_client_key is not None:
            self.logger.info("Using JWT API authentication")
            self.base_url = config.get("base_url")
            self.auth = None
        else:
            self.logger.info("Using Basic Auth API authentication")
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

    def _headers(self, headers, method, path, params):
        headers = headers.copy()
        if self.user_agent:
            headers["User-Agent"] = self.user_agent

        if self.is_cloud:
            # Add OAuth Headers
            headers['Accept'] = 'application/json'
            headers['Authorization'] = 'Bearer {}'.format(self.access_token)
        elif self.jwt_client_key is not None:
            # Add JWT headers
            if params is not None:
                queryParams = []
                for key, value in params.items():
                    queryParams.append('{}={}'.format(key, value))
                path = '{}?{}'.format(path, '&'.join(queryParams))
            jwtToken = self._generateJwtToken(method, path)
            headers['Authorization'] = 'JWT {}'.format(jwtToken)

        return headers

    def _generateJwtToken(self, method, path):
        return atlassian_jwt.encode_token(
            method,
            path,
            self.jwt_client_key,
            self.jwt_shared_secret,
            6 * 60 * 60 # timeout in seconds = 6 hours
        )

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError, HTTPError),
                          jitter=None,
                          max_tries=6,
                          giveup=lambda e: not should_retry_httperror(e))
    def send(self, method, path, headers={}, **kwargs):
        if self.is_cloud or self.jwt_client_key is not None:
            # JWT or OAuth Path
            request = requests.Request(method,
                                       self.url(path),
                                       headers=self._headers(headers, method, path, kwargs.get('params')),
                                       **kwargs)
        else:
            # Basic Auth Path
            request = requests.Request(method,
                                       self.url(path),
                                       auth=self.auth,
                                       headers=self._headers(headers, method, path, kwargs.get('params')),
                                       **kwargs)
        return self.session.send(request.prepare())

    def request(self, tap_stream_id, *args, **kwargs):
        max_tries = 10
        
        for attempt in range(max_tries):
            # Honor rate limiting delay
            wait = (self.next_request_at - datetime.now()).total_seconds()
            if wait > 0:
                time.sleep(wait)
            
            # Make the request
            with metrics.http_request_timer(tap_stream_id) as timer:
                response = self.send(*args, **kwargs)
                self.next_request_at = datetime.now() + TIME_BETWEEN_REQUESTS
                timer.tags[metrics.Tag.http_status_code] = response.status_code
            
            # Handle rate limiting (429)
            if response.status_code == 429:
                if attempt >= max_tries - 1:
                    # Final attempt - give up
                    raise RateLimitException()
                
                # Try to get sleep time from Retry-After header
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        sleep_seconds = int(retry_after)
                        self.logger.info(f"Rate limited. Waiting {sleep_seconds}s as per Retry-After header (attempt {attempt + 1}/{max_tries})")
                    except (ValueError, TypeError):
                        sleep_seconds = 60 * (2 ** attempt)  # Exponential backoff fallback
                        self.logger.info(f"Rate limited. Invalid Retry-After header, using exponential backoff: {sleep_seconds}s (attempt {attempt + 1}/{max_tries})")
                else:
                    sleep_seconds = 60 * (2 ** attempt)  # Exponential backoff
                    self.logger.info(f"Rate limited. Using exponential backoff: {sleep_seconds}s (attempt {attempt + 1}/{max_tries})")
                
                time.sleep(sleep_seconds)
                continue
            
            # Log error responses for debugging
            if response.text and response.status_code >= 400:
                self.logger.warn('Response body: {}'.format(response.text))
            
            # Raise for any other HTTP errors
            response.raise_for_status()
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
            self.logger.info("Starting new login timer")
            self.login_timer = threading.Timer(REFRESH_TOKEN_EXPIRATION_PERIOD,
                                               self.refresh_credentials)
            self.login_timer.start()

    def test_credentials_are_authorized(self):
        # Test with the new enhanced search API endpoint
        body = {
            "jql": "ORDER BY created DESC",
            "maxResults": 1,
            "fields": ["id"]
        }
        self.request("issues", "POST", "/rest/api/3/search/jql", json=body)

    def bulk_fetch_issues(self, tap_stream_id, issue_ids, fields=None):
        """
        Bulk fetch issue details using the /rest/api/3/issue/bulkfetch endpoint.
        
        :param tap_stream_id: Stream ID for metrics
        :param issue_ids: List of issue IDs to fetch
        :param fields: List of fields to return (defaults to ["*all"])
        :return: List of issue objects
        """
        if fields is None:
            fields = ["*all"]
            
        body = {
            "issueIdsOrKeys": issue_ids,
            "fields": fields
        }
        
        self.logger.info(f"Bulk fetching {len(issue_ids)} issues with fields: {fields}")
        
        response = self.request(
            tap_stream_id,
            "POST", 
            "/rest/api/3/issue/bulkfetch",
            json=body
        )
        
        return response.get("issues", [])

    def bulk_fetch_changelogs(self, tap_stream_id, issue_ids, field_ids=None):
        """
        Bulk fetch changelogs using the /rest/api/3/changelog/bulkfetch endpoint.
        
        :param tap_stream_id: Stream ID for metrics
        :param issue_ids: List of issue IDs to fetch changelogs for (max 1000)
        :param field_ids: List of field IDs to filter by (optional, max 10)
        :return: Generator yielding changelog pages
        """
        if len(issue_ids) > 1000:
            raise ValueError("Cannot fetch changelogs for more than 1000 issues at once")
        
        if field_ids and len(field_ids) > 10:
            raise ValueError("Cannot filter by more than 10 field IDs")
        
        next_page_token = None
        page_count = 0
        
        while True:
            body = {
                "issueIdsOrKeys": issue_ids,
                "maxResults": 10000  # this is the max number of reults that can be fetched in one request
            }
            
            if field_ids:
                body["fieldIds"] = field_ids
                
            if next_page_token:
                body["nextPageToken"] = next_page_token
            
            page_count += 1
            if page_count == 1:
                self.logger.info(f"Fetching changelogs for {len(issue_ids)} issues")
            
            response = self.request(
                tap_stream_id,
                "POST", 
                "/rest/api/3/changelog/bulkfetch",
                json=body
            )
            
            # Extract changelogs from the bulk response structure
            issue_change_logs = response.get("issueChangeLogs", [])
            all_changelogs = []
            
            for issue_changelog in issue_change_logs:
                issue_id = issue_changelog.get("issueId")
                change_histories = issue_changelog.get("changeHistories", [])
                
                # Add issueId to each changelog entry for consistency with existing format
                for changelog in change_histories:
                    changelog["issueId"] = issue_id
                    
                    # Convert Unix timestamp (milliseconds) to ISO datetime string if needed
                    if "created" in changelog and isinstance(changelog["created"], (int, float)):
                        # Convert milliseconds to seconds, then to ISO format
                        timestamp_seconds = changelog["created"] / 1000
                        changelog["created"] = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc).isoformat()
                    
                    all_changelogs.append(changelog)
            
            if all_changelogs:
                self.logger.info(f"Found {len(all_changelogs)} changelogs from {len(issue_change_logs)} issues in page {page_count}")
                yield all_changelogs
            
            # Check for next page - break if no nextPageToken or if it's the same as previous
            new_next_page_token = response.get("nextPageToken")
            if not new_next_page_token or new_next_page_token == next_page_token:
                self.logger.info(f"Completed changelog pagination after {page_count} pages for {len(issue_ids)} issues")
                break
            
            next_page_token = new_next_page_token


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


class EnhancedSearchPaginator():
    """
    Specialized paginator for the new Jira enhanced search API (/rest/api/3/search/jql).
    Uses nextPageToken instead of startAt for pagination and POST requests with JSON bodies.
    """
    def __init__(self, client, max_results=100, ids_only=False):
        self.client = client
        self.max_results = max_results
        self.next_page_token = None
        self.ids_only = ids_only

    def pages(self, tap_stream_id, jql, fields=None):
        """Returns a generator which yields pages of issues from the enhanced search API.
        
        :param tap_stream_id: Stream ID for metrics
        :param jql: JQL query string
        :param fields: List of fields to return (defaults to ["*all"] unless ids_only=True)
        """
        # For ID-only fetches, don't specify fields for maximum performance
        if self.ids_only:
            fields = None
        elif fields is None:
            fields = ["*all"]

        while True:
            # Build the request body
            body = {
                "jql": jql,
                "maxResults": self.max_results
            }
            
            # Only add fields parameter if we need field data
            if fields is not None:
                body["fields"] = fields
            
            if self.next_page_token:
                body["nextPageToken"] = self.next_page_token

            # Make POST request with JSON body
            response = self.client.request(
                tap_stream_id, 
                "POST", 
                "/rest/api/3/search/jql",
                json=body
            )

            # Extract issues from response
            issues = response.get("issues", [])
            
            # Update pagination token
            self.next_page_token = response.get("nextPageToken")

            # Yield the page if it has issues
            if issues:
                yield issues

            # Stop if no more pages
            if not self.next_page_token:
                break
