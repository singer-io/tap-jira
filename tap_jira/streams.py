import json
import pytz
import singer
from singer import metrics, utils, metadata, Transformer
from .http import Paginator
from .context import Context

def format_dt(dict_, key):
    str_ = dict_.get(key)
    if not str_:
        return
    dt = utils.strptime_to_utc(str_)
    dict_[key] = utils.strftime(dt)


LOGGER = singer.get_logger()

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
        stream = Context.get_catalog_entry(self.tap_stream_id)
        stream_metadata = metadata.to_map(stream.metadata)
        extraction_time = singer.utils.now()
        for rec in page:
            with Transformer() as transformer:
                rec = transformer.transform(rec, stream.schema.to_dict(), stream_metadata)
            singer.write_record(self.tap_stream_id, rec, time_extracted=extraction_time)
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))


class Versions(Stream):
    def format_versions(self, versions):
        for version in versions:
            format_dt(version, "releaseDate")
            format_dt(version, "startDate")
            format_dt(version, "userStartDate")
            format_dt(version, "userReleaseDate")

    def sync(self, project):
        path = "/rest/api/2/project/{}/version".format(project["id"])
        pager = Paginator(Context.client, order_by="sequence")
        for page in pager.pages(self.tap_stream_id, "GET", path):
            self.format_versions(page)
            self.write_page(page)

VERSIONS = Versions("versions", ["id"], indirect_stream=True)


class Projects(Stream):
    def sync(self):
        projects = Context.client.request(
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
        if Context.is_selected(VERSIONS.tap_stream_id):
            for project in projects:
                VERSIONS.sync(project=project)


class Everything(Stream):
    def __init__(self, *args, path, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path

    def sync(self):
        page = Context.client.request(self.tap_stream_id, "GET", self.path)
        self.write_page(page)


class ProjectTypes(Stream):
    def sync(self):
        path = "/rest/api/2/project/type"
        types = Context.client.request(self.tap_stream_id, "GET", path)
        for type_ in types:
            type_.pop("icon")
        self.write_page(types)


class Users(Stream):
    def _paginate(self, page_num):
        max_results = 2
        params = {"username": "%",
                  "includeInactive": "true",
                  "maxResults": max_results}
        next_page_num = page_num
        while next_page_num is not None:
            params["startAt"] = next_page_num * max_results
            page = Context.client.request(self.tap_stream_id,
                                      "GET",
                                      "/rest/api/2/user/search",
                                      params=params)
            if len(page) < max_results:
                next_page_num = None
            else:
                next_page_num += 1
            if page:
                yield page, next_page_num

    def sync(self):
        params = {"username": "%", "includeInactive": "true"}
        page_num_offset = [self.tap_stream_id, "offset", "page_num"]
        page_num = Context.bookmark(page_num_offset) or 0
        for page, next_page_num in self._paginate(page_num=page_num):
            self.write_page(page)
            Context.set_bookmark(page_num_offset, next_page_num)
            singer.write_state(Context.state)
        Context.set_bookmark(page_num_offset, None)
        singer.write_state(Context.state)


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

    def sync(self):
        updated_bookmark = [self.tap_stream_id, "updated"]
        page_num_offset = [self.tap_stream_id, "offset", "page_num"]

        last_updated = Context.update_start_date_bookmark(updated_bookmark)
        timezone = Context.retrieve_timezone()
        start_date = last_updated.astimezone(pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M")

        jql = "updated >= '{}' order by updated asc".format(start_date)
        params = {"fields": "*all",
                  "expand": "changelog,transitions",
                  "validateQuery": "strict",
                  "jql": jql}
        page_num = Context.bookmark(page_num_offset) or 0
        pager = Paginator(Context.client, items_key="issues", page_num=page_num)
        for page in pager.pages(self.tap_stream_id,
                                "GET", "/rest/api/2/search",
                                params=params):
            # sync comments and changelogs for each issue
            self.sync_sub_streams(page)
            # sync issues
            self.format_issues(page)
            self.write_page(page)

            last_updated = utils.strptime_to_utc(page[-1]["fields"]["updated"])
            Context.set_bookmark(page_num_offset, pager.next_page_num)
            singer.write_state(Context.state)
        Context.set_bookmark(page_num_offset, None)
        Context.set_bookmark(updated_bookmark, last_updated)
        singer.write_state(Context.state)

    def sync_sub_streams(self, page):
        for issue in page:
            comments = issue["fields"].pop("comment")["comments"]
            if comments and Context.is_selected(ISSUE_COMMENTS.tap_stream_id):
                ISSUE_COMMENTS.format_comments(issue, comments)
                ISSUE_COMMENTS.write_page(comments)
            changelogs = issue.pop("changelog")["histories"]
            if changelogs and Context.is_selected(CHANGELOGS.tap_stream_id):
                CHANGELOGS.format_changelogs(issue, changelogs)
                CHANGELOGS.write_page(changelogs)
            transitions = issue.pop("transitions")
            if transitions and Context.is_selected(ISSUE_TRANSITIONS.tap_stream_id):
                ISSUE_TRANSITIONS.format_transitions(issue, transitions)
                ISSUE_TRANSITIONS.write_page(transitions)

ISSUES = Issues("issues", ["id"])


class Worklogs(Stream):
    def _fetch_ids(self, last_updated):
        # since_ts uses millisecond precision
        since_ts = int(last_updated.timestamp()) * 1000
        return Context.client.request(
            self.tap_stream_id,
            "GET",
            "/rest/api/2/worklog/updated",
            params={"since": since_ts},
        )

    def _fetch_worklogs(self, ids):
        if not ids:
            return []
        return Context.client.request(
            self.tap_stream_id, "POST", "/rest/api/2/worklog/list",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"ids": ids}),
        )

    def format_worklogs(self, worklogs):
        for worklog in worklogs:
            format_dt(worklog, "created")
            format_dt(worklog, "updated")
            format_dt(worklog, "started")

    def raise_if_bookmark_cannot_advance(self, worklogs):
        # Worklogs can only be queried with a `since` timestamp and
        # provides no way to page through the results. The `since`
        # timestamp has <=, not <, semantics. It also caps the response at
        # 1000 objects. Because of this, if we ever see a page of 1000
        # worklogs that all have the same `updated` timestamp, we cannot
        # tell whether we in fact got all the updates and so we need to
        # raise.
        #
        # That said, a page of 999 worklogs that all have the same
        # timestamp is fine. That just means that 999 worklogs were
        # updated at the same timestamp but that we did, in fact, get them
        # all.
        #
        # The behavior, then, always resyncs the latest `updated`
        # timestamp, no matter how many results are there. If you have 500
        # worklogs updated at T1 and 999 worklogs updated at T2 and
        # `last_updated` is set to T1, the first trip through this will
        # see 1000 items, 500 of which have `updated==T1` and 500 of which
        # have `updated==T2`. Then, `last_updated` is set to T2 and due to
        # the <= semantics, you grab the 999 T2 worklogs which passes this
        # function because there's less than 1000 worklogs of
        # `updated==T2`.
        #
        # OTOH, if you have 1 worklog with `updated==T1` and 1000 worklogs
        # with `updated==T2`, first trip you see 1 worklog at T1 and 999
        # at T2 which this code will think is fine, but second trip
        # through you'll see 1000 worklogs at T2 which will fail
        # validation (because we can't tell whether there would be more
        # that should've been returned).
        LOGGER.debug('Worklog page count: `%s`', len(worklogs))
        worklog_updatedes = [utils.strptime_to_utc(w['updated'])
                             for w in worklogs]
        min_updated = min(worklog_updatedes)
        max_updated = max(worklog_updatedes)
        LOGGER.debug('Worklog min updated: `%s`', min_updated)
        LOGGER.debug('Worklog max updated: `%s`', max_updated)
        if len(worklogs) == 1000 and min_updated == max_updated:
            raise Exception(("Worklogs bookmark can't safely advance."
                             "Every `updated` field is `{}`")
                            .format(worklog_updatedes[0]))

    def advance_bookmark(self, worklogs):
        self.raise_if_bookmark_cannot_advance(worklogs)
        new_last_updated = max(utils.strptime_to_utc(w["updated"])
                               for w in worklogs)
        return new_last_updated

    def sync(self):
        updated_bookmark = [self.tap_stream_id, "updated"]
        last_updated = Context.update_start_date_bookmark(updated_bookmark)
        while True:
            ids_page = self._fetch_ids(last_updated)
            if not ids_page["values"]:
                break
            ids = [x["worklogId"] for x in ids_page["values"]]
            worklogs = self._fetch_worklogs(ids)
            self.format_worklogs(worklogs)
            self.write_page(worklogs)

            new_last_updated = self.advance_bookmark(worklogs)
            last_updated = new_last_updated
            Context.set_bookmark(updated_bookmark, last_updated)
            singer.write_state(Context.state)
            # lastPage is a boolean value based on
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/?utm_source=%2Fcloud%2Fjira%2Fplatform%2Frest%2F&utm_medium=302#api-api-3-worklog-updated-get
            last_page = ids_page.get("lastPage")
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


def validate_dependencies():
    errs = []
    selected = [s.tap_stream_id for s in Context.catalog.streams
                if Context.is_selected(s.tap_stream_id)]
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
