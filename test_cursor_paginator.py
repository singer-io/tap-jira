#!/usr/bin/env python3

class MockClient:
    def __init__(self, responses):
        self.responses = responses
        self.call_count = 0
        self.requests = []
    
    def request(self, *args, **kwargs):
        params = kwargs.get('params', {})
        self.requests.append(params.copy())
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

# Test cursor pagination with v3/search/api response format
print("Testing CursorPaginator with v3/search/api endpoint:")

responses = [
    {
        "isLast": False,
        "issues": [{"key": "PROJ-1"}, {"key": "PROJ-2"}],
        "nextPageToken": "token123"
    },
    {
        "isLast": True,
        "issues": [{"key": "PROJ-3"}]
    }
]

client = MockClient(responses)
paginator = CursorPaginator(client, items_key="issues")

pages = list(paginator.pages("issues", "GET", "/rest/api/3/search/jql",
                            params={"jql": "project = PROJ", "maxResults": 2}))

print(f"Pages retrieved: {len(pages)}")
print(f"Total issues: {sum(len(page) for page in pages)}")
print(f"Requests made: {len(client.requests)}")
print(f"Used nextPageToken: {'nextPageToken' in str(client.requests)}")
print(f"Request 1 params: {client.requests[0]}")
print(f"Request 2 params: {client.requests[1]}")
print("\n✅ CursorPaginator test completed successfully!")
