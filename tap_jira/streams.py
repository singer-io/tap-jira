import contextvars
import functools
import json
import threading
import pytz
import requests
import singer
import datetime

from singer import metrics, utils, metadata, Transformer, Timer
from .http import Paginator
from .context import Context
from itertools import chain
from concurrent.futures import ThreadPoolExecutor, as_completed

from minware_singer_utils import SecureLogger

LOGGER = SecureLogger(singer.get_logger())

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


def should_exclude_field(field_id, field_name):
    excluded_fields = Context.get_exclude_issue_fields()
    if field_id in excluded_fields:
        return True
    
    if field_id.startswith('customfield_'):
        custom_field_id = field_id[len('customfield_'):]
        if field_name.rstrip('_' + custom_field_id) in excluded_fields: 
            return True
    
    return False

def sync_sub_streams(page, issue_changelog_updated):
    for issue in page:
        comments = issue["fields"].pop("comment")["comments"]
        if comments and Context.is_selected(ISSUE_COMMENTS.tap_stream_id):
            for comment in comments:
                comment["issueId"] = issue["id"]
            ISSUE_COMMENTS.write_page(comments)

        if Context.is_selected(CHANGELOGS.tap_stream_id):
            changelog_response = issue.pop("changelog")
            changelogs = changelog_response["histories"]
            changelogs_to_write = []

            # when expanding changelogs for an issue, jira returns 100
            if changelog_response['maxResults'] >= changelog_response['total']:
                for changelog in changelogs:
                    changelogs_to_write.append(changelog)
            else:
                pager = Paginator(Context.client)
                for page in pager.pages(
                    CHANGELOGS.tap_stream_id,
                    "GET",
                    "/rest/api/2/issue/{}/changelog".format(issue["id"])
                ):
                    for changelog in page:
                        changelogs_to_write.append(changelog)


            for changelog in changelogs_to_write:
                changelog_items = []
                for item in changelog['items']:
                    if 'fieldId' in item and should_exclude_field(item['fieldId'], item['field']):
                        if item['from'] is not None and item['from'] != '':
                            item['from'] = '<REDACTED>'
                        if item['fromString'] is not None and item['fromString'] != '':
                            item['fromString'] = '<REDACTED>'
                        if item['to'] is not None and item['to'] != '':
                            item['to'] = '<REDACTED>'
                        if item['toString'] is not None and item['toString'] != '':
                            item['toString'] = '<REDACTED>'
                        
                    changelog_items.append(item)
                changelog['items'] = changelog_items

            CHANGELOGS.write_page(
                [{ **changelog, 'issueId': issue["id"] } for changelog in changelogs_to_write]
            )

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


class Projects(Stream):
    def sync(self, getAll = False):
        projects = Context.client.request(
            self.tap_stream_id, "GET", "/rest/api/2/project",
            params={"expand": "description,lead,url,projectKeys"})
        # apply projects filter if applicable
        projectsFilter = '' if getAll else Context.get_projects()
        if len(projectsFilter) > 0:
            # remove all project ids and keys when:
            # 1. the project doesn't exist in list of projects
            # 2. the project is archived and therefore unqueryable
            allPossibleProjectFilterValues = list(chain.from_iterable(
                (p["id"], p["key"]) for p in projects if 'archived' not in p or p['archived'] == False
            ))
            projectsFilter = [pf for pf in projectsFilter if pf in allPossibleProjectFilterValues]
            newProjectsConfig = ','.join(projectsFilter)
            if newProjectsConfig != ','.join(Context.get_projects()):
                LOGGER.warn('projects config contains unavailable projects: \n\t{}\n\tUPDATED TO\n\t{}'.format(Context.config["projects"], newProjectsConfig))
                # other streams will not check for project availability explicitly, so update the config here
                Context.config["projects"] = newProjectsConfig
            projects = list(filter(lambda p: p["key"] in projectsFilter or p["id"] in projectsFilter, projects))

        # If just fetching all the projects, return early
        if getAll:
            return projects

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
                    # `userReleaseDate` and `userStartDate` is a localized string
                    # the schema has its data type as date-time.
                    #
                    # To avoid problems with non-english user settings
                    # we override this with the actual date-time
                    for version in page:
                        version['userReleaseDate'] = version.get('releaseDate')
                        version['userStartDate'] = version.get('startDate')
                    VERSIONS.write_page(page)
        if Context.is_selected(COMPONENTS.tap_stream_id):
            for project in projects:
                path = "/rest/api/2/project/{}/component".format(project["id"])
                pager = Paginator(Context.client)
                for page in pager.pages(COMPONENTS.tap_stream_id, "GET", path):
                    COMPONENTS.write_page(page)

class ProjectsNormalized(Stream):
    def sync(self):
        projects = Context.client.request(
            self.tap_stream_id, "GET", "/rest/api/2/project",
            params={"expand": "description"})

        # apply projects filter if applicable
        projectsFilter = Context.get_projects()
        if len(projectsFilter) > 0:
            projects = list(filter(lambda p: p["key"] in projectsFilter, projects))

        # produce normalized project objects
        normalizedProjects = []
        for project in projects:
            normalizedProjects.append({
                "id": project["id"],
                "name": "{} ({})".format(project["name"], project["key"]), # e.g. "minware (MW)"
                "description": project["description"],
                # we have to build the URL because it doesnt come back from the API
                "url": "{}/browse/{}".format(Context.config["base_url"].rstrip("/"), project["key"])
            })

        self.write_page(normalizedProjects)


class ProjectTypes(Stream):
    def sync(self):
        path = "/rest/api/2/project/type"
        types = Context.client.request(self.tap_stream_id, "GET", path)
        for type_ in types:
            type_.pop("icon")
        self.write_page(types)


class Boards(Stream):
    def sync(self):
        path = "/rest/agile/1.0/board"
        # Just do full sync each time for now
        params = {}
        pager = Paginator(Context.client, items_key='values')
        for page in pager.pages(self.tap_stream_id, "GET", path, params=params):
            self.write_page(page)


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
            try:
                params = {"groupname": group,
                          "maxResults": max_results,
                          "includeInactiveUsers": True}
                pager = Paginator(Context.client, items_key='values')
                for page in pager.pages(self.tap_stream_id, "GET",
                                        "/rest/api/2/group/member",
                                        params=params):
                    self.write_page(page)
            except requests.exceptions.HTTPError as http_error:
                if http_error.response.status_code == 404:
                    LOGGER.info("Could not find group \"%s\", skipping", group)
                else:
                    raise http_error


class Issues(Stream):
    # All projects bookmark key must not conflict with potential
    # jira project keys and jira project ids.
    ALL_PROJECTS_BOOKMARK_KEY = '0_ALL_PROJECTS'

    def __init__(self, tap_stream_id, pk_fields, indirect_stream=False, path=None):
        super().__init__(tap_stream_id, pk_fields, indirect_stream, path)
        self.write_lock = threading.Lock()

    def sync(self):
        stream = Context.get_catalog_entry(self.tap_stream_id)
        knownFields = stream.schema.properties['fields'].properties

        # First, fetch all the custom field names for translation
        fieldNames = {}
        fields = []
        for field in Context.client.request('issue_fields', "GET", "/rest/api/2/field"):
            fields.append(field)

        # When generating fieldNames, we need to get all the system fields first
        # In the event that customfields_* has an identical name to a system field,
        # we need to make sure we do not overwrite the system field with the customfields value
        sortedFields = sorted(fields, key=lambda f: f['custom'], reverse=False)
        for field in sortedFields:
            id = field['id']
            name = field['name']

            # JIRA does allow custommfields to have names that conflict
            # with each other as well as system fields.
            # If we run into this problem, we append the customfields
            # numeric id to the name to avoid collisions
            if name in fieldNames.values() or name in knownFields.keys():
                name += '_' + field['id'].replace('customfield_', '')

            fieldNames[id] = name

        projectsToSync = Context.get_projects()
        if len(projectsToSync) == 0:
            with Timer('issues_sync', { 'project': self.ALL_PROJECTS_BOOKMARK_KEY }):
                self.sync_project(fieldNames, knownFields)
        else:
            with ThreadPoolExecutor(max_workers=6) as executor:
                func_call_futures = []
                for project_key_or_id in projectsToSync:
                    ctx = contextvars.copy_context()
                    func_call = functools.partial(ctx.run, self.sync_project, fieldNames, knownFields, project_key_or_id)
                    func_call_futures.append(executor.submit(func_call))

                # bubble up any exceptions discovered while syncing a project
                try:
                    for future in as_completed(func_call_futures):
                        future.result()
                except Exception as ex:
                    LOGGER.error(
                        "Issues.sync encountered an error in a thread: %s", ex
                    )
                    raise ex

        self.delete_old_state()

    def delete_old_state(self):
        Context.set_bookmark([self.tap_stream_id, "updated"], None)
        Context.set_bookmark([self.tap_stream_id, "offset"], None)

    def check_and_migrate_state(self, updated_bookmark, page_num_offset):
        project_updated_bookmark = Context.bookmark(updated_bookmark)
        project_page_num_offset = Context.bookmark(page_num_offset)

        # check if project_page_num_offset is a Dict
        # we are getting an exception below with the logger call 
        # when the value is not iterable
        if not isinstance(project_page_num_offset, dict):
            project_page_num_offset_for_logger = {
                'unknown': 'unknown'
            }
        else:
            project_page_num_offset_for_logger = project_page_num_offset

        LOGGER.info('Checking state {}: {} and {}: {}'.format(
            '.'.join(updated_bookmark), project_updated_bookmark,
            '.'.join(project_page_num_offset_for_logger), project_page_num_offset
        ))

        if not project_page_num_offset and not project_updated_bookmark:
            non_project_updated_bookmark_key = [self.tap_stream_id, "updated"]
            non_project_updated_bookmark = Context.bookmark(non_project_updated_bookmark_key)
            LOGGER.info('Previous state found {}: {}'.format('.'.join(non_project_updated_bookmark_key), non_project_updated_bookmark))
            if non_project_updated_bookmark:
                LOGGER.info('Updated being copied from previous state format')
                Context.set_bookmark(updated_bookmark, non_project_updated_bookmark)
                Context.set_bookmark(page_num_offset, 0)

    def sync_project(self, fieldNames, knownFields, project_key_or_id = None):
        if project_key_or_id is None:
            project_key_or_id = self.ALL_PROJECTS_BOOKMARK_KEY

        LOGGER.info('Begin syncing issues for project {}'.format(project_key_or_id))

        # build projects filter from config, if any
        projectsJql = "" if project_key_or_id == self.ALL_PROJECTS_BOOKMARK_KEY \
            else "project IN ({}) and ".format(project_key_or_id)

        updated_bookmark = [self.tap_stream_id, project_key_or_id, "updated"]
        page_num_offset = [self.tap_stream_id, project_key_or_id, "offset", "page_num"]

        self.check_and_migrate_state(updated_bookmark, page_num_offset)

        last_updated = Context.update_start_date_bookmark(updated_bookmark)
        timezone = Context.retrieve_timezone()
        start_date = last_updated.astimezone(pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M")
        if datetime.datetime(2024, 7, 15, 0, 0, tzinfo=pytz.utc) < last_updated < datetime.datetime(2024, 8, 10, 5, 0, 0, tzinfo=pytz.utc):
            LOGGER.info('state is in broken timeframe, going back in time to ensure all issues are ingested')
            start_date = datetime.datetime(2024, 7, 15, 0, 0, tzinfo=pytz.utc).strftime("%Y-%m-%d %H:%M")

        issue_changelogs_updated_bookmark_path = [CHANGELOGS.tap_stream_id, project_key_or_id, "updated"]
        issue_changelogs_updated = Context.update_start_date_bookmark(issue_changelogs_updated_bookmark_path)
        if datetime.datetime(2024, 7, 15, 0, 0, tzinfo=pytz.utc) < issue_changelogs_updated < datetime.datetime(2024, 8, 10, 5, 0, 0, tzinfo=pytz.utc):
            LOGGER.info('changelog state is in broken timeframe, going back in time to ensure all issues are ingested')
            start_date = datetime.datetime(2024, 7, 15, 0, 0, tzinfo=pytz.utc).strftime("%Y-%m-%d %H:%M")
            issue_changelogs_updated = datetime.datetime(2024, 7, 15, 0, 0, tzinfo=pytz.utc)

        # grab the time now, before we sync any changelogs
        # this will be used later to bookmark our progress
        # there will be some overlap during the sync, but this allows us to avoid saving state per issue
        issue_changelogs_sync_time = utils.now()

        LOGGER.info('using updated >= \'{}\''.format(start_date))

        # Now fetch all the actual issues, translating custom fields
        jql = "{} updated >= '{}' order by updated asc".format(projectsJql, start_date).strip()
        params = {"fields": "*all",
                  "expand": "changelog,transitions",
                  "validateQuery": "strict",
                  "maxResults": 100,
                  "jql": jql}
        page_num = Context.bookmark(page_num_offset) or 0
        pager = Paginator(Context.client, items_key="issues", page_num=page_num)

        page_index = 0
        for page in pager.pages(self.tap_stream_id,
                                "GET", "/rest/api/2/search",
                                params=params):

            LOGGER.info(
                "Fetched page %d with %d issues for project %s",
                page_index, len(page), project_key_or_id
            )
            # sync comments and changelogs for each issue
            sync_sub_streams(page, issue_changelogs_updated)
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

                # Rename all of the custom fields
                # filter excluded fields
                for k in list(issue['fields'].keys()):
                    if k[:len('customfield_')] == 'customfield_':
                        val = issue['fields'][k]
                        del issue['fields'][k]
                        issue['fields'][fieldNames[k]] = val

                    if fieldNames[k] in issue['fields'] and should_exclude_field(k, fieldNames[k]):
                        LOGGER.debug('Excluding field {} - {}'.format(k, fieldNames[k]))
                        issue['fields'][fieldNames[k]] = '<REDACTED>'

                # Now, go through and separate fields we don't recognize into "_custom"
                customFields = {}
                for k in list(issue['fields'].keys()):
                    # If we don't know about this field, then put it in a "_custom" object for
                    # outputting as a single JSON
                    if not k in knownFields:
                        val = issue['fields'][k]
                        # Don't include null values, which just waste a bunch of space
                        if val != None:
                            customFields[k] = val
                        del issue['fields'][k]
                issue['fields']['_custom'] = json.dumps(customFields)


            # Grab last_updated before transform in write_page
            last_updated = utils.strptime_to_utc(page[-1]["fields"]["updated"])
            LOGGER.info("Writing issues for page %d, project %s...", page_index, project_key_or_id)
            with self.write_lock:
                self.write_page(page)

                Context.set_bookmark(page_num_offset, pager.next_page_num)
                singer.write_state(Context.state)
            
            LOGGER.info("Finished writing issues for page %d, project %s", page_index, project_key_or_id)
            page_index += 1
        
        # After the loop completes
        with self.write_lock:
            Context.set_bookmark(page_num_offset, None)
            Context.set_bookmark(updated_bookmark, last_updated)
            Context.set_bookmark(issue_changelogs_updated_bookmark_path, issue_changelogs_sync_time)
            singer.write_state(Context.state)

        LOGGER.info('Done syncing project %s', project_key_or_id)


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


class WorklogsDeleted(Stream):
    def sync(self):
        updated_bookmark = [self.tap_stream_id, "updated"]
        last_updated = Context.update_start_date_bookmark(updated_bookmark)
        since_ts = int(last_updated.timestamp()) * 1000
        while since_ts is not None:
            records_page = Context.client.request(
                self.tap_stream_id,
                "GET",
                "/rest/api/2/worklog/deleted",
                params={"since": since_ts},
            )

            if not records_page.get("values"):
                break

            self.write_page(records_page.get("values"))

            # store bookmark in ISO-8601 format, which requires conversion from the Unix timestamp
            # that the worklog records have
            max_updated_time = (records_page.get("until") / 1000)
            last_updated = datetime.datetime.utcfromtimestamp(max_updated_time).isoformat() + "Z"
            Context.set_bookmark(updated_bookmark, last_updated)
            singer.write_state(Context.state)
            # lastPage is a boolean value based on
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/?utm_source=%2Fcloud%2Fjira%2Fplatform%2Frest%2F&utm_medium=302#api-api-3-worklog-updated-get
            last_page = records_page.get("lastPage")
            if last_page:
                break

            since_ts = records_page.get("until")

VERSIONS = Stream("versions", ["id"], indirect_stream=True)
COMPONENTS = Stream("components", ["id"], indirect_stream=True)
ISSUES = Issues("issues", ["id"])
ISSUE_COMMENTS = Stream("issue_comments", ["id"], indirect_stream=True)
ISSUE_TRANSITIONS = Stream("issue_transitions", ["id"],
                           indirect_stream=True)
PROJECTS = Projects("projects", ["id"])
PROJECTS_NORMALIZED = ProjectsNormalized("projects_normalized", ["id"])
CHANGELOGS = Stream("changelogs", ["id"], indirect_stream=True)

ALL_STREAMS = [
    PROJECTS,
    PROJECTS_NORMALIZED,
    VERSIONS,
    COMPONENTS,
    ProjectTypes("project_types", ["key"]),
    Boards("boards", ["id"]),
    Stream("project_categories", ["id"], path="/rest/api/2/projectCategory"),
    Stream("resolutions", ["id"], path="/rest/api/2/resolution"),
    Stream("roles", ["id"], path="/rest/api/2/role"),
    Stream("priorities", ["id"], path="/rest/api/2/priority"),
    Users("users", ["accountId"]),
    ISSUES,
    ISSUE_COMMENTS,
    CHANGELOGS,
    ISSUE_TRANSITIONS,
    Worklogs("worklogs", ["id"]),
    WorklogsDeleted("worklogs_deleted", ["worklogId"]),
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
