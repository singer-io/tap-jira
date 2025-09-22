import unittest
import requests
import json
from unittest import mock
from tap_jira import http
from tap_jira import streams
from tap_jira.context import Context

def get_projects_response(is_last=False, on_prem=False):
    '''Get th projects response for the projects stream.'''
    return_response = {}
    values = []
    if not on_prem:
        return_response['values'] = values
        return_response['isLast'] = is_last # indicates if it is the last page.
    print(return_response)
    return return_response

class MockStreams():
    def __init__(self):
        return None

# Mock response
def get_mock_http_response(status_code, content={}):
    contents = json.dumps(content)
    response = requests.Response()
    response.status_code = status_code
    response.headers = {}
    response._content = contents.encode()
    response.url = ""
    response.request = requests.Request()
    response.request.method = ""
    return response

first_page = get_projects_response(is_last=False)
last_page = get_projects_response(is_last=True)
on_prem_page = get_projects_response(on_prem=True)
cloud_resp = {"deploymentType": "Cloud"}
on_prem_resp = {"deploymentType": "Server"}

class TestProjectsPagination(unittest.TestCase):

    @mock.patch("tap_jira.http.Client.request", side_effect = [cloud_resp,first_page, last_page])
    @mock.patch('tap_jira.context.Context.get_catalog_entry')
    def test_projects_stream_pagination(self, mock_catalog_entry, mock_request):
        '''Verify that the pagination works correctly with correct page size and breaks when breaking condition occurs'''
        mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
        Context.client = http.Client(mock_config)
        mock_stream = MockStreams()
        MockStreams.streams = "projects"
        Context.catalog = [mock_stream] # setting the context catalog
        projects = streams.Projects('projects', ['id'], "INCREMENTAL")
        projects.sync()

        self.assertEqual([
            mock.call('users', 'GET', '/rest/api/2/serverInfo'), # call for getting server info
            mock.call('projects', 'GET', '/rest/api/2/project/search', params={'expand': 'description,lead,url,projectKeys', 'maxResults': 50, 'startAt': 0}), # page 1 call
            mock.call('projects', 'GET', '/rest/api/2/project/search', params={'expand': 'description,lead,url,projectKeys', 'maxResults': 50, 'startAt': 50}) # page 2 call
            ], mock_request.mock_calls)

class TestProjectsEndpointForSync(unittest.TestCase):
    @mock.patch("tap_jira.http.Client.request", side_effect = [cloud_resp, last_page])
    @mock.patch('tap_jira.context.Context.get_catalog_entry')
    def test_projects_sync_cloud(self, mock_catalog_entry, mock_request):
        '''Verify that project/search endpoint is called for cloud server'''
        mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
        Context.client = http.Client(mock_config)
        mock_stream = MockStreams()
        MockStreams.streams = "projects"
        Context.catalog = [mock_stream] # setting the context catalog
        projects = streams.Projects('projects', ['id'], "INCREMENTAL")
        projects.sync()
        print(last_page)
        print(mock_request.mock_calls)

        self.assertEqual(
            mock.call('projects', 'GET', '/rest/api/2/project/search', params={'expand': 'description,lead,url,projectKeys', 'maxResults': 50, 'startAt': 0}), # verify it calls project/search endpoint
            mock_request.mock_calls[1])

    @mock.patch("tap_jira.http.Client.request", side_effect = [on_prem_resp, on_prem_page])
    @mock.patch('tap_jira.context.Context.get_catalog_entry')
    def test_projects_sync_on_prem(self, mock_catalog_entry, mock_request):
        '''Verify that the project endpoint is called for on_prem server'''
        mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
        Context.client = http.Client(mock_config)
        mock_stream = MockStreams()
        MockStreams.streams = "projects"
        Context.catalog = [mock_stream] # setting the context catalog
        projects = streams.Projects('projects', ['id'], "INCREMENTAL")
        projects.sync()
        print(mock_request.mock_calls)

        self.assertEqual(
            mock.call('projects', 'GET', '/rest/api/2/project', params={'expand': 'description,lead,url,projectKeys'}), # verify it calls the project endpoint
            mock_request.mock_calls[1])
