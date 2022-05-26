import json
import pytz
import singer
import dateparser

from singer import metrics, utils, metadata, Transformer
from .http import Paginator,JiraNotFoundError
from .context import Context

DEFAULT_PAGE_SIZE = 50

def raise_if_bookmark_cannot_advance(worklogs):
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


def sync_sub_streams(page):
    for issue in page:
        comments = issue["fields"].pop("comment")["comments"]
        if comments and Context.is_selected(ISSUE_COMMENTS.tap_stream_id):
            for comment in comments:
                comment["issueId"] = issue["id"]
            ISSUE_COMMENTS.write_page(comments)
        changelogs = issue.pop("changelog")["histories"]
        if changelogs and Context.is_selected(CHANGELOGS.tap_stream_id):
            for changelog in changelogs:
                changelog["issueId"] = issue["id"]
            CHANGELOGS.write_page(changelogs)
        transitions = issue.pop("transitions")
        if transitions and Context.is_selected(ISSUE_TRANSITIONS.tap_stream_id):
            for transition in transitions:
                transition["issueId"] = issue["id"]
            ISSUE_TRANSITIONS.write_page(transitions)


def advance_bookmark(worklogs):
    raise_if_bookmark_cannot_advance(worklogs)
    new_last_updated = max(utils.strptime_to_utc(w["updated"])
                           for w in worklogs)
    return new_last_updated


LOGGER = singer.get_logger()


class Stream():
    """Information about and functions for syncing streams for the Jira API.

    Important class properties:

    :var tap_stream_id:
    :var pk_fields: A list of primary key fields
    :var indirect_stream: If True, this indicates the stream cannot be synced
    directly, but instead has its data generated via a separate stream."""
    def __init__(self, tap_stream_id, pk_fields, indirect_stream=False, path=None):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields
        # Only used to skip streams in the main sync function
        self.indirect_stream = indirect_stream
        self.path = path

    def __repr__(self):
        return "<Stream(" + self.tap_stream_id + ")>"

    def sync(self):
        page = Context.client.request(self.tap_stream_id, "GET", self.path)
        self.write_page(page)

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

def update_user_date(page):
    """
    Transform date value to 'yyyy-mm-dd' format.
    API returns userReleaseDate and userStartDate always in the dd/mm/yyyy format where the month name is in Abbreviation form.
    Dateparser library handles locale value and converts Abbreviation month to number.
    For example, if userReleaseDate is 12/abr/2022 then we are converting it to 2022-04-12.
    """
    if page.get('userReleaseDate'):
        page['userReleaseDate'] = transform_user_date(page['userReleaseDate'])
    if page.get('userStartDate'):
        page['userStartDate'] = transform_user_date(page['userStartDate'])

    return page
class Projects(Stream):
    def sync_on_prem(self):
        """ Sync function for the on prem instances"""
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
                path = "/rest/api/2/project/{}/version".format(project["id"])
                pager = Paginator(Context.client, order_by="sequence")
                for page in pager.pages(VERSIONS.tap_stream_id, "GET", path):
                    # Transform userReleaseDate and userStartDate values to 'yyyy-mm-dd' format.
                    for each_page in page:
                        each_page = update_user_date(each_page)
                    VERSIONS.write_page(page)
        if Context.is_selected(COMPONENTS.tap_stream_id):
            for project in projects:
                path = "/rest/api/2/project/{}/component".format(project["id"])
                pager = Paginator(Context.client)
                for page in pager.pages(COMPONENTS.tap_stream_id, "GET", path):
                    COMPONENTS.write_page(page)

    def sync_cloud(self):
        """ Sync function for the cloud instances"""
        offset = 0
        while True:
            params = {
                "expand": "description,lead,url,projectKeys",
                "maxResults": DEFAULT_PAGE_SIZE, # maximum number of results to fetch in a page.
                "startAt": offset #the offset to start at for the next page
            }
            projects = Context.client.request(
                self.tap_stream_id, "GET", "/rest/api/2/project/search",
                params=params)
            for project in projects.get('values'):
                # The Jira documentation suggests that a "versions" key may appear
                # in the project, but from my testing that hasn't been the case
                # (even when projects do have versions). Since we are already
                # syncing versions separately, pop this key just in case it
                # appears.
                project.pop("versions", None)
            self.write_page(projects.get('values'))
            if Context.is_selected(VERSIONS.tap_stream_id):
                for project in projects.get('values'):
                    path = "/rest/api/2/project/{}/version".format(project["id"])
                    pager = Paginator(Context.client, order_by="sequence")
                    for page in pager.pages(VERSIONS.tap_stream_id, "GET", path):
                        # Transform userReleaseDate and userStartDate values to 'yyyy-mm-dd' format.
                        for each_page in page:
                            each_page = update_user_date(each_page)

                        VERSIONS.write_page(page)
            if Context.is_selected(COMPONENTS.tap_stream_id):
                for project in projects.get('values'):
                    path = "/rest/api/2/project/{}/component".format(project["id"])
                    pager = Paginator(Context.client)
                    for page in pager.pages(COMPONENTS.tap_stream_id, "GET", path):
                        COMPONENTS.write_page(page)

            # `isLast` corresponds to whether it is the last page or not.
            if projects.get("isLast"):
                break
            offset = offset + DEFAULT_PAGE_SIZE # next offset to start from

    def sync(self):
        # The documentation https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-projects/#api-rest-api-3-project-get
        # suggests that the rest/api/3/project endpoint would be deprecated from the version 3 and w could use project/search endpoint
        # which gives paginated response. However, the on prem servers doesn't allow working on the project/search endpoint. Hence for the cloud
        # instances, the new endpoint would be called which also suggests pagination, but for on prm instances the old endpoint would be called.
        # As we want to include both the cloud as well as the on-prem servers.
        if Context.client.is_on_prem_instance:
            self.sync_on_prem()
        else:
            self.sync_cloud()

class ProjectTypes(Stream):
    def sync(self):
        path = "/rest/api/2/project/type"
        types = Context.client.request(self.tap_stream_id, "GET", path)
        for type_ in types:
            type_.pop("icon")
        self.write_page(types)


class Users(Stream):
    def sync(self):
        max_results = 2

        if Context.config.get("groups"):
            groups = Context.config.get("groups").split(",")
        else:
            groups = ["jira-administrators",
                      "jira-software-users",
                      "jira-core-users",
                      "jira-users",
                      "users"]

        for group in groups:
            group = group.strip()
            try:
                params = {"groupname": group,
                          "maxResults": max_results,
                          "includeInactiveUsers": True}
                pager = Paginator(Context.client, items_key='values')
                for page in pager.pages(self.tap_stream_id, "GET",
                                        "/rest/api/2/group/member",
                                        params=params):
                    self.write_page(page)
            except JiraNotFoundError:
                LOGGER.info("Could not find group \"%s\", skipping", group)


class Issues(Stream):

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
            sync_sub_streams(page)
            for issue in page:
                issue['fields'].pop('worklog', None)
                # The JSON schema for the search endpoint indicates an "operations"
                # field can be present. This field is self-referential, making it
                # difficult to deal with - we would have to flatten the operations
                # and just have each operation include the IDs of other operations
                # it references. However the operations field has something to do
                # with the UI within Jira - I believe the operations are parts of
                # the "menu" bar for each issue. This is of questionable utility,
                # so we decided to just strip the field out for now.
                issue['fields'].pop('operations', None)

            # Grab last_updated before transform in write_page
            last_updated = utils.strptime_to_utc(page[-1]["fields"]["updated"])

            self.write_page(page)

            Context.set_bookmark(page_num_offset, pager.next_page_num)
            singer.write_state(Context.state)
        Context.set_bookmark(page_num_offset, None)
        Context.set_bookmark(updated_bookmark, last_updated)
        singer.write_state(Context.state)


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

    def sync(self):
        updated_bookmark = [self.tap_stream_id, "updated"]
        last_updated = Context.update_start_date_bookmark(updated_bookmark)
        while True:
            ids_page = self._fetch_ids(last_updated)
            if not ids_page["values"]:
                break
            ids = [x["worklogId"] for x in ids_page["values"]]
            worklogs = self._fetch_worklogs(ids)

            # Grab last_updated before transform in write_page
            new_last_updated = advance_bookmark(worklogs)

            self.write_page(worklogs)

            last_updated = new_last_updated
            Context.set_bookmark(updated_bookmark, last_updated)
            singer.write_state(Context.state)
            # lastPage is a boolean value based on
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/?utm_source=%2Fcloud%2Fjira%2Fplatform%2Frest%2F&utm_medium=302#api-api-3-worklog-updated-get
            last_page = ids_page.get("lastPage")
            if last_page:
                break


VERSIONS = Stream("versions", ["id"], indirect_stream=True)
COMPONENTS = Stream("components", ["id"], indirect_stream=True)
ISSUES = Issues("issues", ["id"])
ISSUE_COMMENTS = Stream("issue_comments", ["id"], indirect_stream=True)
ISSUE_TRANSITIONS = Stream("issue_transitions", ["id","issueId"], # Composite primary key
                           indirect_stream=True)
PROJECTS = Projects("projects", ["id"])
CHANGELOGS = Stream("changelogs", ["id"], indirect_stream=True)

ALL_STREAMS = [
    PROJECTS,
    VERSIONS,
    COMPONENTS,
    ProjectTypes("project_types", ["key"]),
    Stream("project_categories", ["id"], path="/rest/api/2/projectCategory"),
    Stream("resolutions", ["id"], path="/rest/api/2/resolution"),
    Stream("roles", ["id"], path="/rest/api/2/role"),
    Users("users", ["accountId"]),
    ISSUES,
    ISSUE_COMMENTS,
    CHANGELOGS,
    ISSUE_TRANSITIONS,
    Worklogs("worklogs", ["id"]),
]

ALL_STREAM_IDS = [s.tap_stream_id for s in ALL_STREAMS]


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
    if COMPONENTS.tap_stream_id in selected and PROJECTS.tap_stream_id not in selected:
        errs.append(msg_tmpl.format("Components", "Projects"))
    if ISSUES.tap_stream_id not in selected:
        if CHANGELOGS.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Changelog", "Issues"))
        if ISSUE_COMMENTS.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Issue Comments", "Issues"))
        if ISSUE_TRANSITIONS.tap_stream_id in selected:
            errs.append(msg_tmpl.format("Issue Transitions", "Issues"))
    if errs:
        raise DependencyException(" ".join(errs))

def transform_user_date(user_date):
    """
    Transform date value to 'yyyy-mm-dd' format.
    API returns userReleaseDate and userStartDate always in the dd/mm/yyyy format where the month name is in Abbreviation form.
    Dateparser library handles locale value and converts Abbreviation month to number.
    For example, if userReleaseDate is 12/abr/2022 then we are converting it to 2022-04-12.
    Then, at the end singer-python will transform any DateTime to %Y-%m-%dT00:00:00Z format.

    All the locales are supported except following below locales,
    Chinese, Italia, Japanese, Korean, Polska, Brasil.
    """
    return dateparser.parse(user_date).strftime('%Y-%m-%d')
