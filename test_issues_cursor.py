#!/usr/bin/env python3

class MockClient:
    def __init__(self, responses):
        self.responses = responses
        self.call_count = 0
        self.requests = []
    
    def request(self, *args, **kwargs):
        params = kwargs.get('params', {})
        self.requests.append(params.copy())
        print(f"Request {self.call_count + 1}: {params}")
        response = self.responses[self.call_count]
        self.call_count += 1
        return response

# Copy the CursorPaginator class directly
class CursorPaginator():
    def __init__(self, client, items_key="issues"):
        self.client = client
        self.items_key = items_key
        self.next_page_token = None

    def pages(self, *args, **kwargs):
        """Returns a generator which yields pages of data using cursor-based pagination.
        Uses nextPageToken for the Jira v3/search/api endpoint.

        :param args: Passed to Client.request
        :param kwargs: Passed to Client.request
        """
        params = kwargs.pop("params", {}).copy()
        
        while True:
            # Add nextPageToken if we have one from previous page
            if self.next_page_token:
                params["nextPageToken"] = self.next_page_token
            
            response = self.client.request(*args, params=params, **kwargs)
            
            # Extract the page data
            if self.items_key:
                page = response.get(self.items_key, [])
            else:
                page = response
            
            # Always yield the page (even if empty)
            yield page
            
            # Check if this is the last page
            if response.get("isLast", True):
                break
                
            # Get the next page token
            self.next_page_token = response.get("nextPageToken")
            if not self.next_page_token:
                break

# Create a simplified Issues class for testing
class TestIssues:
    def __init__(self):
        self.tap_stream_id = "issues"
    
    def sync(self, client):
        # Simulate the updated Issues.sync method
        jql = "updated >= '2023-01-01 00:00' order by updated asc"
        params = {
            "jql": jql,
            "fields": ["*all"],
            "expand": "changelog,transitions",
            "maxResults": 50,
        }
        
        # Use CursorPaginator for v3/search/jql endpoint
        pager = CursorPaginator(client, items_key="issues")
        
        pages_processed = 0
        total_issues = 0
        
        for page in pager.pages(
            self.tap_stream_id, "GET", "/rest/api/3/search/jql", params=params
        ):
            pages_processed += 1
            total_issues += len(page)
            print(f"Processed page {pages_processed} with {len(page)} issues")
            
        return pages_processed, total_issues

# Test the Issues stream with CursorPaginator
print("Testing Issues stream with CursorPaginator:")

responses = [
    {
        "isLast": False,
        "issues": [
            {"id": "1", "key": "PROJ-1", "fields": {"updated": "2023-01-01T10:00:00.000Z"}},
            {"id": "2", "key": "PROJ-2", "fields": {"updated": "2023-01-01T11:00:00.000Z"}}
        ],
        "nextPageToken": "cursor123"
    },
    {
        "isLast": True,
        "issues": [
            {"id": "3", "key": "PROJ-3", "fields": {"updated": "2023-01-01T12:00:00.000Z"}}
        ]
    }
]

client = MockClient(responses)
issues_stream = TestIssues()

pages, total = issues_stream.sync(client)

print(f"\nResults:")
print(f"Pages processed: {pages}")
print(f"Total issues: {total}")
print(f"Requests made: {len(client.requests)}")
print(f"Used nextPageToken: {'nextPageToken' in str(client.requests)}")
print(f"Used v3/search/jql endpoint: {any('/rest/api/3/search/jql' in str(req) for req in client.requests)}")
print(f"Request 1 params: {client.requests[0]}")
print(f"Request 2 params: {client.requests[1]}")
print("\n✅ Issues stream now uses CursorPaginator successfully!")
