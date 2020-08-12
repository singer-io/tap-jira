import unittest
from tap_jira import utils, streams
from singer import Catalog, CatalogEntry, Schema


class TestFlattenStreams(unittest.TestCase):
    nested = {
        'boards': streams.STREAMS['boards'],
        'issues': streams.STREAMS['issues'],
        'worklogs': streams.STREAMS['worklogs']
    }

    not_nested = {
        'worklogs': streams.STREAMS['worklogs']
    }

    def test_flattens_nested(self):
        flattened = utils.flatten_streams(self.nested, {})
        self.assertEquals(len(flattened.keys()), 10)

    def test_not_nested(self):
        flattened = utils.flatten_streams(self.not_nested, {})
        self.assertEquals(len(flattened.keys()), 1)


class TestDeepGet(unittest.TestCase):
    dummy_dict = {
        'main': {
            'sub': 'value'
        }
    }

    def test_get_first_level(self):
        val = utils.deep_get(self.dummy_dict, 'main')
        self.assertEquals(val, self.dummy_dict['main'])

    def test_get_levels(self):
        val = utils.deep_get(self.dummy_dict, 'main.sub')
        self.assertEquals(val, 'value')


class TestRaiseIfBookmarkCannotAdvance(unittest.TestCase):
    dummy_worklog = {
        "updated": "2020-07-24T06:28:45.782+0000"
    }

    def test_raise_exception(self):
        thousand_worklogs = [self.dummy_worklog for _ in range(1000)]
        self.assertRaises(Exception,
                          utils.raise_if_bookmark_cannot_advance, thousand_worklogs)


class TestValidateDependencies(unittest.TestCase):
    catalog = Catalog([
        CatalogEntry(tap_stream_id='boards', schema=Schema(),
                     metadata=[{'metadata': {'selected': False}, 'breadcrumb': []}]),
        CatalogEntry(tap_stream_id='issue_board', schema=Schema(),
                     metadata=[{'metadata': {'selected': True}, 'breadcrumb': []}]),
        CatalogEntry(tap_stream_id='project_board', schema=Schema(),
                     metadata=[{'metadata': {'selected': False}, 'breadcrumb': []}]),
        CatalogEntry(tap_stream_id='epics', schema=Schema(),
                     metadata=[{'metadata': {'selected': False}, 'breadcrumb': []}]),
        CatalogEntry(tap_stream_id='sprints', schema=Schema(),
                     metadata=[{'metadata': {'selected': True}, 'breadcrumb': []}]),
        CatalogEntry(tap_stream_id='issue_comments', schema=Schema(),
                     metadata=[{'metadata': {'selected': True}, 'breadcrumb': []}]),
    ])

    def test_is_selected(self):
        selected = utils.is_selected(streams.IssueBoard, self.catalog)
        self.assertTrue(selected)

    def test_raises_substream_error(self):
        test_streams = {
            'boards': streams.STREAMS['boards']
        }
        # test recursive checking
        test_streams['boards']['substreams']['issues'] = streams.STREAMS['issues']
        self.assertRaises(
            utils.DependencyException,
            utils.validate_dependencies,
            test_streams, self.catalog
        )

    def test_raises_right_amount_of_substream_errors(self):
        test_streams = {
            'boards': streams.STREAMS['boards']
        }
        # test recursive checking
        test_streams['boards']['substreams']['issues'] = streams.STREAMS['issues']
        with self.assertRaises(utils.DependencyException) as context:
            utils.validate_dependencies(test_streams, self.catalog)
            self.assertTrue(len(context.exception.errors) == 3)
