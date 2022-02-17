import unittest
from unittest import mock
from tap_jira import http
from tap_jira import streams
from tap_jira.context import Context

def get_projects_response(is_last=False):
    '''Get th projects response for the projects stream.'''
    return_response = {}
    values = []
    for i in range(0, streams.DEFAULT_PAGE_SIZE):
        project = {"id": "{}".format(i),
                   "key": "key_{}".format(i)}
        values.append(project)
    return_response['values'] = values
    return_response['isLast'] = is_last # indicates if it is the last page.
    return return_response

class MockStreams():
    def __init__(self):
        return None

first_page = get_projects_response(is_last=False)
last_page = get_projects_response(is_last=True)

class TestProjectsPagination(unittest.TestCase):

    @mock.patch("tap_jira.http.Client.request", side_effect = [{},first_page, last_page])
    @mock.patch('tap_jira.context.Context.get_catalog_entry')
    def test_projects_stream_pagination(self, mock_catalog_entry, mock_request):
        '''Verify that the pagination works correctly with correct page size and breaks when breaking condition occurs'''
        mock_config = {"username":"mock_username","password":"mock_password","base_url": "mock_base_url"}
        Context.client = http.Client(mock_config)
        mock_stream = MockStreams()
        MockStreams.streams = "projects"
        Context.catalog = [mock_stream] # setting the context catalog
        projects = streams.Projects('projects', ['id'])
        projects.sync()

        self.assertEqual([
            mock.call('test', 'GET', '/rest/api/2/myself'), # call for test creds
            mock.call('projects', 'GET', '/rest/api/2/project/search', params={'expand': 'description,lead,url,projectKeys', 'maxResults': 50, 'startAt': 0}), # page 1 call
            mock.call('projects', 'GET', '/rest/api/2/project/search', params={'expand': 'description,lead,url,projectKeys', 'maxResults': 50, 'startAt': 50}) # page 2 call
            ], mock_request.mock_calls)
