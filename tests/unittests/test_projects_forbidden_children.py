import unittest
from unittest import mock

from tap_jira import http, streams

SEARCH = "/rest/api/2/project/search"
PROJECT = "/rest/api/2/project"


def versions_path(project_id):
    return "/rest/api/2/project/{}/version".format(project_id)


def components_path(project_id):
    return "/rest/api/2/project/{}/component".format(project_id)


def forbidden():
    return http.JiraForbiddenError("HTTP-error-code: 403, Error: forbidden")


def page(*ids, max_results=50, **extra):
    """A paginated `values` response. A page shorter than `max_results`
    is the last one."""
    values = [dict({"id": i}, **extra) for i in ids]
    return {"values": values, "maxResults": max_results}


def project_page(*ids, is_last):
    return {"values": [{"id": i} for i in ids], "isLast": is_last}


def make_request(routes, calls):
    """Build a `Client.request` stand-in. `routes` maps (path, startAt) to a
    response, or to an exception instance that is raised. Any request that is
    not listed fails the test, so unexpected calls are caught."""
    def _request(tap_stream_id, method, path, **kwargs):
        start_at = (kwargs.get("params") or {}).get("startAt")
        calls.append((path, start_at))
        key = (path, start_at)
        if key not in routes:
            raise AssertionError("Unexpected request: {}".format(key))
        result = routes[key]
        if isinstance(result, Exception):
            raise result
        return result
    return _request


class ProjectsForbiddenChildrenBase(unittest.TestCase):
    on_prem = False

    def setUp(self):
        client_patcher = mock.patch("tap_jira.streams.Context.client")
        self.mock_client = client_patcher.start()
        self.addCleanup(client_patcher.stop)
        self.mock_client.is_on_prem_instance = self.on_prem

        write_patcher = mock.patch("tap_jira.streams.Stream.write_page", autospec=True)
        self.mock_write = write_patcher.start()
        self.addCleanup(write_patcher.stop)

        self.selected = {"versions", "components"}
        selected_patcher = mock.patch("tap_jira.streams.Context.is_selected",
                                      side_effect=lambda sid: sid in self.selected)
        selected_patcher.start()
        self.addCleanup(selected_patcher.stop)

        self.calls = []

    def run_sync(self, routes):
        self.mock_client.request.side_effect = make_request(routes, self.calls)
        streams.Projects("projects", ["id"], "FULL_TABLE").sync()

    def written(self, stream_id):
        """IDs of the records written for `stream_id`, one list per page."""
        return [[record["id"] for record in call.args[1]]
                for call in self.mock_write.call_args_list
                if call.args[0].tap_stream_id == stream_id]


class ForbiddenChildrenScenarios:
    """Scenarios shared by Cloud and on-prem. Subclasses provide
    `project_routes(*ids)` for the projects endpoint, and `PROJECT_IDS`."""

    PROJECT_IDS = ("1", "2", "3")

    def child_routes(self, versions=None, components=None):
        """Default to every project having one accessible version and
        component; `versions`/`components` override specific project IDs."""
        routes = {}
        for pid in self.PROJECT_IDS:
            routes[(versions_path(pid), 0)] = page("v" + pid)
            routes[(components_path(pid), 0)] = page("c" + pid)
        for pid, result in (versions or {}).items():
            routes[(versions_path(pid), 0)] = result
        for pid, result in (components or {}).items():
            routes[(components_path(pid), 0)] = result
        return routes

    def routes(self, **overrides):
        routes = self.project_routes(*self.PROJECT_IDS)
        routes.update(self.child_routes(**overrides))
        return routes

    # --- 403 on a child stream ------------------------------------------

    def test_versions_forbidden_skips_only_that_project(self):
        self.run_sync(self.routes(versions={"1": forbidden()}))

        self.assertEqual(self.written("versions"), [["v2"], ["v3"]])
        # components of every project, including project 1, are still synced
        self.assertEqual(self.written("components"), [["c1"], ["c2"], ["c3"]])
        # all projects were written
        self.assertEqual([i for p in self.written("projects") for i in p], ["1", "2", "3"])

    def test_components_forbidden_skips_only_that_project(self):
        self.run_sync(self.routes(components={"2": forbidden()}))

        self.assertEqual(self.written("components"), [["c1"], ["c3"]])
        self.assertEqual(self.written("versions"), [["v1"], ["v2"], ["v3"]])

    def test_versions_forbidden_does_not_affect_components_of_same_project(self):
        self.run_sync(self.routes(versions={pid: forbidden() for pid in self.PROJECT_IDS}))

        self.assertEqual(self.written("versions"), [])
        self.assertEqual(self.written("components"), [["c1"], ["c2"], ["c3"]])

    def test_both_children_forbidden_still_writes_projects(self):
        self.run_sync(self.routes(versions={"1": forbidden()}, components={"1": forbidden()}))

        self.assertEqual([i for p in self.written("projects") for i in p], ["1", "2", "3"])
        self.assertEqual(self.written("versions"), [["v2"], ["v3"]])
        self.assertEqual(self.written("components"), [["c2"], ["c3"]])

    def test_forbidden_on_later_page_keeps_records_already_written(self):
        routes = self.routes(versions={"1": page("v1a", "v1b", max_results=2)})
        routes[(versions_path("1"), 2)] = forbidden()
        self.run_sync(routes)

        # page one of project 1 is kept, its failing page two is skipped,
        # and the following projects still sync
        self.assertEqual(self.written("versions"), [["v1a", "v1b"], ["v2"], ["v3"]])
        self.assertEqual(self.written("components"), [["c1"], ["c2"], ["c3"]])

    def test_components_forbidden_on_later_page_keeps_records_already_written(self):
        routes = self.routes(components={"2": page("c2a", "c2b", max_results=2)})
        routes[(components_path("2"), 2)] = forbidden()
        self.run_sync(routes)

        self.assertEqual(self.written("components"), [["c1"], ["c2a", "c2b"], ["c3"]])

    def test_forbidden_child_is_logged_with_stream_and_project(self):
        with mock.patch("tap_jira.streams.LOGGER") as mock_logger:
            self.run_sync(self.routes(versions={"2": forbidden()}))

        self.assertEqual(mock_logger.warning.call_count, 1)
        args = mock_logger.warning.call_args.args
        self.assertIn("versions", args)
        self.assertIn("2", args)

    # --- errors that must NOT be tolerated ------------------------------

    def test_other_errors_on_children_are_not_suppressed(self):
        for error in (http.JiraBadRequestError("400"),
                      http.JiraNotFoundError("404"),
                      http.JiraInternalServerError("500")):
            for kind in ("versions", "components"):
                with self.subTest(error=type(error).__name__, child=kind):
                    self.mock_write.reset_mock()
                    self.calls.clear()
                    with self.assertRaises(type(error)):
                        self.run_sync(self.routes(**{kind: {"1": error}}))

    def test_unexpected_non_jira_error_is_not_suppressed(self):
        with self.assertRaises(RuntimeError):
            self.run_sync(self.routes(versions={"1": RuntimeError("boom")}))

    # --- Projects endpoint itself ---------------------------------------

    def test_forbidden_projects_endpoint_propagates(self):
        """A 403 on the Projects request is for the main sync loop to handle."""
        self.mock_client.request.side_effect = forbidden()

        with self.assertRaises(http.JiraForbiddenError):
            streams.Projects("projects", ["id"], "FULL_TABLE").sync()
        self.mock_write.assert_not_called()

    # --- behaviour that must be preserved -------------------------------

    def test_nothing_forbidden_syncs_everything(self):
        self.run_sync(self.routes())

        self.assertEqual(self.written("versions"), [["v1"], ["v2"], ["v3"]])
        self.assertEqual(self.written("components"), [["c1"], ["c2"], ["c3"]])

    def test_unselected_children_are_not_requested(self):
        self.selected = set()
        self.run_sync(self.project_routes(*self.PROJECT_IDS))

        self.assertEqual(self.written("versions"), [])
        self.assertEqual(self.written("components"), [])
        self.assertFalse([c for c in self.calls if "/version" in c[0] or "/component" in c[0]])

    def test_version_dates_are_still_transformed(self):
        routes = self.routes(versions={
            "1": page("v1", userReleaseDate="12/Apr/2022", userStartDate="01/Jan/2022")})
        self.run_sync(routes)

        first_version = next(call.args[1][0] for call in self.mock_write.call_args_list
                             if call.args[0].tap_stream_id == "versions")
        self.assertEqual(first_version["userReleaseDate"], "2022-04-12")
        self.assertEqual(first_version["userStartDate"], "2022-01-01")


class TestCloudProjectsForbiddenChildren(ForbiddenChildrenScenarios,
                                         ProjectsForbiddenChildrenBase):
    """Cloud: projects come from the paginated /project/search endpoint, here
    across two pages (projects 1 and 2, then project 3)."""
    on_prem = False

    def project_routes(self, *ids):
        return {
            (SEARCH, 0): project_page(*ids[:2], is_last=False),
            (SEARCH, 50): project_page(*ids[2:], is_last=True),
        }

    def test_forbidden_child_on_first_project_page_does_not_skip_later_project_pages(self):
        """The Copilot review scenario: versions 403 on the first project page."""
        self.run_sync(self.routes(versions={"1": forbidden(), "2": forbidden()}))

        # project page 2 was still requested and its project fully synced
        self.assertIn((SEARCH, 50), self.calls)
        self.assertEqual(self.written("versions"), [["v3"]])
        self.assertEqual(self.written("components"), [["c1"], ["c2"], ["c3"]])
        self.assertEqual(self.written("projects"), [["1", "2"], ["3"]])

    def test_pagination_requests_are_unchanged(self):
        self.run_sync(self.routes())

        project_requests = [c for c in self.mock_client.request.call_args_list
                            if c.args[2] == SEARCH]
        self.assertEqual([c.kwargs["params"]["startAt"] for c in project_requests], [0, 50])
        self.assertEqual(project_requests[0].kwargs["params"]["maxResults"], 50)

    def test_forbidden_on_second_projects_page_propagates_after_first_is_written(self):
        routes = self.routes()
        routes[(SEARCH, 50)] = forbidden()

        with self.assertRaises(http.JiraForbiddenError):
            self.run_sync(routes)
        self.assertEqual(self.written("projects"), [["1", "2"]])


class TestOnPremProjectsForbiddenChildren(ForbiddenChildrenScenarios,
                                          ProjectsForbiddenChildrenBase):
    """On-prem: /project returns every project at once, unpaginated."""
    on_prem = True

    def project_routes(self, *ids):
        return {(PROJECT, None): [{"id": i} for i in ids]}
