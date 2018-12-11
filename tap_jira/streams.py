import json
import pytz
import singer
from singer import metrics, metadata, Transformer
from singer.utils import strftime
import pendulum
from .http import Paginator
from .context import Context

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
            Context.write_state()
        Context.set_bookmark(page_num_offset, None)
        Context.write_state()


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


def translate_keys(fields, names):
    for key, value in fields.items():
        # There exists a custom name mapping and its a custom field
        if names.get(key) and key[0:11] == "customfield":
            replacement_key = names[key]
            fields[replacement_key] = value
            fields.pop(key)
    return fields # or an updated copy


class Issues(Stream):
    def format_issues(self, issues):
        for issue in issues:

            names = issue['names']
            fields = translate_keys(issue["fields"], names)

            # Sanitize on our end or let the loaders do it
            import ipdb; ipdb.set_trace()
            1+1
            # This shouldn't be necessary...
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
        start_date = pendulum.parse(last_updated).astimezone(pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M")
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
            last_updated = page[-1]["fields"]["updated"]
            Context.set_bookmark(page_num_offset, pager.next_page_num)
            Context.write_state()
        Context.set_bookmark(page_num_offset, None)
        Context.set_bookmark(updated_bookmark, last_updated)
        Context.write_state()

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
        since_ts = int(pendulum.parse(last_updated).timestamp()) * 1000
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
            Context.set_bookmark(updated_bookmark, last_updated)
            Context.write_state()
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
