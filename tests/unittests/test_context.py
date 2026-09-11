import unittest
from types import SimpleNamespace

from tap_jira.context import Context


class TestIsSelectedMissingStream(unittest.TestCase):
    '''
        Verify `Context.is_selected` treats a stream absent from the catalog
        (e.g. a cloud-only stream excluded for an on-prem instance) as not
        selected, rather than raising a KeyError.
    '''

    def setUp(self):
        # Reset the memoized stream_map so each test builds it from `catalog`
        Context.stream_map = {}

    def tearDown(self):
        Context.stream_map = {}
        Context.catalog = None

    def test_is_selected_returns_false_for_stream_not_in_catalog(self):
        Context.catalog = SimpleNamespace(streams=[
            SimpleNamespace(tap_stream_id="projects", metadata=[]),
        ])

        self.assertFalse(Context.is_selected("groups"))

    def test_get_catalog_entry_returns_none_for_stream_not_in_catalog(self):
        Context.catalog = SimpleNamespace(streams=[
            SimpleNamespace(tap_stream_id="projects", metadata=[]),
        ])

        self.assertIsNone(Context.get_catalog_entry("group_users"))
