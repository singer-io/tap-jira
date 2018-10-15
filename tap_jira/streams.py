import json
import pytz
import singer
from singer import metrics
from singer.utils import strftime
import pendulum
from .http import Paginator


def format_dt(dict_, key):
    str_ = dict_.get(key)
    if not str_:
        return
    dt = pendulum.parse(str_).in_timezone("UTC")
    dict_[key] = strftime(dt)


class Stream(object):
    """Information about and functions for syncing streams for the Jira API.

    Important class properties:

    :var tap_stream_id:
    :var pk_fields: A list of primary key fields
    :var indirect_stream: If True, this indicates the stream cannot be synced
    directly, but instead has its data generated via a separate stream."""
    def __init__(self, tap_stream_id, pk_fields, indirect_stream=False):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields
        self.indirect_stream = indirect_stream

    def __repr__(self):
        return "<Stream(" + self.tap_stream_id + ")>"

    def write_page(self, page):
        singer.write_records(self.tap_stream_id, page)
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))


class Versions(Stream):
    def format_versions(self, versions):
        for version in versions:
            format_dt(version, "releaseDate")
            format_dt(version, "startDate")
            format_dt(version, "userStartDate")
            format_dt(version, "userReleaseDate")

    def sync(self, ctx, *, project):
        path = "/rest/api/2/project/{}/version".format(project["id"])
        pager = Paginator(ctx.client, order_by="sequence")
        for page in pager.pages(self.tap_stream_id, "GET", path):
            self.format_versions(page)
            self.write_page(page)

VERSIONS = Versions("versions", ["id"], indirect_stream=True)


class Projects(Stream):
    def sync(self, ctx):
        projects = ctx.client.request(
            self.tap_stream_id, "GET", "/rest/api/2/project",
            params={"expand": "description,lead,url,projectKeys"})
        for project in projects:
            # The Jira documentation suggests that a "versions" key may appear
            # in the project, but from my testing that hasn't been the case
            # (even when projects do have versions). Since we are already
            # syncing versions separately, pop this key just in case it
            # appears.
            project.pop("versions", None)
        self.write_page(projects)
        if VERSIONS.tap_stream_id in ctx.selected_stream_ids:
            for project in projects:
                VERSIONS.sync(ctx, project=project)


class Everything(Stream):
    def __init__(self, *args, path, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path

    def sync(self, ctx):
        page = ctx.client.request(self.tap_stream_id, "GET", self.path)
        self.write_page(page)


class ProjectTypes(Stream):
    def sync(self, ctx):
        path = "/rest/api/2/project/type"
        types = ctx.client.request(self.tap_stream_id, "GET", path)
        for type_ in types:
            type_.pop("icon")
        self.write_page(types)


class Users(Stream):
    def _paginate(self, ctx, *, page_num):
        max_results = 2
        params = {"username": "%",
                  "includeInactive": "true",
                  "maxResults": max_results}
        next_page_num = page_num
        while next_page_num is not None:
            params["startAt"] = next_page_num * max_results
            page = ctx.client.request(self.tap_stream_id,
                                      "GET",
                                      "/rest/api/2/user/search",
                                      params=params)
            if len(page) < max_results:
                next_page_num = None
            else:
                next_page_num += 1
            if page:
                yield page, next_page_num

    def sync(self, ctx):
        params = {"username": "%", "includeInactive": "true"}
        page_num_offset = [self.tap_stream_id, "offset", "page_num"]
        page_num = ctx.bookmark(page_num_offset) or 0
        for page, next_page_num in self._paginate(ctx, page_num=page_num):
            self.write_page(page)
            ctx.set_bookmark(page_num_offset, next_page_num)
            ctx.write_state()
        ctx.set_bookmark(page_num_offset, None)
        ctx.write_state()


class IssueComments(Stream):
    def format_comments(self, issue, comments):
        for comment in comments:
            comment["issueId"] = issue["id"]
            format_dt(comment, "updated")
            format_dt(comment, "created")

ISSUE_COMMENTS = IssueComments("issue_comments", ["id"], indirect_stream=True)


class IssueTransitions(Stream):
    def format_transitions(self, issue, transitions):
        for transition in transitions:
            transition["issueId"] = issue["id"]

ISSUE_TRANSITIONS = IssueTransitions("issue_transitions", ["id"],
                                     indirect_stream=True)


class Changelogs(Stream):
    def format_changelogs(self, issue, changelogs):
        for changelog in changelogs:
            changelog["issueId"] = issue["id"]
            format_dt(changelog, "created")
            for hist in changelog.get("histories", []):
                format_dt(hist, "created")

CHANGELOGS = Changelogs("changelogs", ["id"], indirect_stream=True)


class Issues(Stream):
    def format_issues(self, issues):
        for issue in issues:
            fields = issue["fields"]
            format_dt(fields, "updated")
            format_dt(fields, "created")
            format_dt(fields, "lastViewed")
            for att in fields.get("attachment", []):
                format_dt(att, "created")
            fields.pop("worklog", None)
            # The JSON schema for the search endpoint indicates an "operations"
            # field can be present. This field is self-referential, making it
            # difficult to deal with - we would have to flatten the operations
            # and just have each operation include the IDs of other operations
            # it references. However the operations field has something to do
            # with the UI within Jira - I believe the operations are parts of
            # the "menu" bar for each issue. This is of questionable utility,
            # so we decided to just strip the field out for now.
            issue.pop("operations", None)

    def sync(self, ctx):
        updated_bookmark = [self.tap_stream_id, "updated"]
        page_num_offset = [self.tap_stream_id, "offset", "page_num"]
        last_updated = ctx.update_start_date_bookmark(updated_bookmark)
        timezone = ctx.retrieve_timezone()
        start_date = pendulum.parse(last_updated).astimezone(pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M")
        jql = "updated >= '{}' order by updated asc".format(start_date)
        params = {"fields": "*all",
                  "expand": "changelog,transitions",
                  "validateQuery": "strict",
                  "jql": jql}
        page_num = ctx.bookmark(page_num_offset) or 0
        pager = Paginator(ctx.client, items_key="issues", page_num=page_num)
        for page in pager.pages(self.tap_stream_id,
                                "GET", "/rest/api/2/search",
                                params=params):
            # sync comments and changelogs for each issue
            self.sync_sub_streams(page, ctx)
            # sync issues
            self.format_issues(page)
            self.write_page(page)
            last_updated = page[-1]["fields"]["updated"]
            ctx.set_bookmark(page_num_offset, pager.next_page_num)
            ctx.write_state()
        ctx.set_bookmark(page_num_offset, None)
        ctx.set_bookmark(updated_bookmark, last_updated)
        ctx.write_state()

    def sync_sub_streams(self, page, ctx):
        for issue in page:
            comments = issue["fields"].pop("comment")["comments"]
            if comments and (ISSUE_COMMENTS.tap_stream_id in ctx.selected_stream_ids):
                ISSUE_COMMENTS.format_comments(issue, comments)
                ISSUE_COMMENTS.write_page(comments)
            changelogs = issue.pop("changelog")["histories"]
            if changelogs and (CHANGELOGS.tap_stream_id in ctx.selected_stream_ids):
                CHANGELOGS.format_changelogs(issue, changelogs)
                CHANGELOGS.write_page(changelogs)
            transitions = issue.pop("transitions")
            if transitions and (ISSUE_TRANSITIONS.tap_stream_id in ctx.selected_stream_ids):
                ISSUE_TRANSITIONS.format_transitions(issue, transitions)
                ISSUE_TRANSITIONS.write_page(transitions)

ISSUES = Issues("issues", ["id"])


class Worklogs(Stream):
    def _fetch_ids(self, ctx, last_updated):
        since_ts = int(pendulum.parse(last_updated).timestamp()) * 1000
        return ctx.client.request(
            self.tap_stream_id,
            "GET",
            "/rest/api/2/worklog/updated",
            params={"since": since_ts},
        )

    def _fetch_worklogs(self, ctx, ids):
        if not ids:
            return []
        return ctx.client.request(
            self.tap_stream_id, "POST", "/rest/api/2/worklog/list",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"ids": ids}),
        )

    def format_worklogs(self, worklogs):
        for worklog in worklogs:
            format_dt(worklog, "created")
            format_dt(worklog, "updated")
            format_dt(worklog, "started")

    def sync(self, ctx):
        updated_bookmark = [self.tap_stream_id, "updated"]
        last_updated = ctx.update_start_date_bookmark(updated_bookmark)
        while True:
            ids_page = self._fetch_ids(ctx, last_updated)
            if not ids_page["values"]:
                break
            ids = [x["worklogId"] for x in ids_page["values"]]
            worklogs = self._fetch_worklogs(ctx, ids)
            self.format_worklogs(worklogs)
            self.write_page(worklogs)
            max_updated = max(w["updated"] for w in worklogs)
            last_page = ids_page.get("lastPage")
            if not last_page and (max_updated <= last_updated):
                # Note: This route doesn't include any way to give a page
                # number. It also only ever returns 1000 items. If there were
                # ever a situation where there were more than 1000 worklogs
                # updated at the same time, this baby would be f-ed. We'll be
                # safe and just make sure we aren't getting the same page
                # repeatedly.
                raise Exception("Page's max updated ({}) <= previous page's ({})"
                                .format(max_updated, last_updated))
            last_updated = max_updated
            ctx.set_bookmark(updated_bookmark, last_updated)
            ctx.write_state()
            if last_page:
                break

PROJECTS = Projects("projects", ["id"])

all_streams = [
    PROJECTS,
    VERSIONS,
    ProjectTypes("project_types", ["key"]),
    Everything("project_categories", ["id"], path="/rest/api/2/projectCategory"),
    Everything("resolutions", ["id"], path="/rest/api/2/resolution"),
    Everything("roles", ["id"], path="/rest/api/2/role"),
    Users("users", ["key"]),
    ISSUES,
    ISSUE_COMMENTS,
    CHANGELOGS,
    ISSUE_TRANSITIONS,
    Worklogs("worklogs", ["id"]),
]
all_stream_ids = [s.tap_stream_id for s in all_streams]


class DependencyException(Exception):
    pass


def validate_dependencies(ctx):
    errs = []
    selected = ctx.selected_stream_ids
    msg_tmpl = ("Unable to extract {0} data. "
                "To receive {0} data, you also need to select {1}.")
    if VERSIONS.tap_stream_id in selected and PROJECTS.tap_stream_id not in selected:
        errs.append(msg_tmpl.format("Versions", "Projects"))
    if ISSUES.tap_stream_id not in selected:
        if CHANGELOGS.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Changelog", "Issues"))
        if ISSUE_COMMENTS.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Issue Comments", "Issues"))
        if ISSUE_TRANSITIONS.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Issue Transitions", "Issues"))
    if errs:
        raise DependencyException(" ".join(errs))
