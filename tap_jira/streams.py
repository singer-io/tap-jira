import contextvars
import functools
import json
import threading
import pytz
import psutil
import requests
import singer
import datetime

from singer import metrics, utils, metadata, Transformer, Timer
from .http import Paginator, EnhancedSearchPaginator
from .context import Context
from itertools import chain
from concurrent.futures import ThreadPoolExecutor, as_completed

from minware_singer_utils import SecureLogger

LOGGER = SecureLogger(singer.get_logger())

def log_memory(message, *args):
    """
    Log current process memory usage with a custom message.
    Returns the current memory in MB for further processing if needed.
    """
    current_process = psutil.Process()
    current_memory_mb = current_process.memory_info().rss / 1024 / 1024
    
    # Format message with args if provided
    if args:
        formatted_message = message % args
    else:
        formatted_message = message
    
    LOGGER.info(f"{formatted_message}: {current_memory_mb:.1f} MB")
    return current_memory_mb

def partition_list(lst, batch_size):
    """Partition a list into batches of specified size."""
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]


def bulk_fetch_issues_parallel(client, tap_stream_id, issue_ids, fields=None, max_workers=10):
    """
    Fetch issue details in parallel using bulk fetch API.
    
    :param client: Jira client instance
    :param tap_stream_id: Stream ID for metrics
    :param issue_ids: List of issue IDs to fetch
    :param fields: List of fields to return
    :param max_workers: Maximum number of parallel workers
    :return: List of all fetched issues
    """
    if not issue_ids:
        return []
    
    # Partition into batches of 100 (max for bulk fetch API)
    batches = list(partition_list(issue_ids, 100))
    LOGGER.info(f"Fetching {len(issue_ids)} issues in {len(batches)} parallel batches")
    
    all_issues = []
    
    def fetch_batch(batch):
        return client.bulk_fetch_issues(tap_stream_id, batch, fields)
    
    # Monitor memory before parallel fetching
    initial_memory_mb = log_memory("Memory before parallel fetch")
    
    # Use ThreadPoolExecutor for parallel fetching
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_batch, batch) for batch in batches]
        
        for future in as_completed(futures):
            try:
                issues = future.result()
                all_issues.extend(issues)
                # Monitor memory after each parallel batch fetch
                log_memory(f"Fetched batch of {len(issues)} issues")
            except Exception as exc:
                LOGGER.error(f"Batch fetch failed: {exc}")
                raise exc
    
    # Monitor memory after all parallel fetching completes
    final_memory_mb = log_memory(f"Completed parallel fetch: {len(all_issues)} total issues")
    memory_growth = final_memory_mb - initial_memory_mb
    LOGGER.info(f"Memory growth during parallel fetch: {memory_growth:.1f} MB")
    return all_issues

def bulk_fetch_changelogs_for_issues(client, tap_stream_id, issue_ids):
    """
    Bulk fetch changelogs for a list of issues using the bulk changelog API.
    
    :param client: Jira client instance
    :param tap_stream_id: Stream ID for metrics
    :param issue_ids: List of issue IDs to fetch changelogs for
    :return: Dictionary mapping issue_id to list of changelogs
    """
    if not issue_ids:
        return {}
    
    # Monitor memory before changelog fetching
    initial_memory_mb = log_memory("Memory before changelog fetch")
    
    changelog_map = {}
    total_changelogs = 0
    
    # Process in batches of 1000 (API limit)
    for batch_index, batch in enumerate(partition_list(issue_ids, 1000)):
        LOGGER.info(f"Bulk fetching changelogs for batch {batch_index} ({len(batch)} issues)")
        
        batch_changelogs = 0
        for changelog_page in client.bulk_fetch_changelogs(tap_stream_id, batch):
            for changelog in changelog_page:
                issue_id = changelog.get("issueId")
                if issue_id:
                    # Ensure consistent string type for issue ID
                    issue_id_str = str(issue_id)
                    if issue_id_str not in changelog_map:
                        changelog_map[issue_id_str] = []
                    changelog_map[issue_id_str].append(changelog)
                    batch_changelogs += 1
                else:
                    LOGGER.warning(f"Changelog missing issueId: {changelog.keys()}")
        
        total_changelogs += batch_changelogs
        # Monitor memory after each batch
        log_memory(f"After changelog batch {batch_index}: {batch_changelogs} changelogs fetched")
    
    # Final memory report
    final_memory_mb = log_memory(f"Bulk fetched {total_changelogs} changelogs for {len(changelog_map)} issues")
    memory_growth = final_memory_mb - initial_memory_mb
    LOGGER.info(f"Memory growth during changelog fetch: {memory_growth:.1f} MB")
    
    return changelog_map

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

def sync_sub_streams(page, issue_changelog_updated, changelog_map=None):
    for issue in page:
        comments = issue["fields"].pop("comment")["comments"]
        if comments and Context.is_selected(ISSUE_COMMENTS.tap_stream_id):
            for comment in comments:
                comment["issueId"] = issue["id"]
            ISSUE_COMMENTS.write_page(comments)

        if Context.is_selected(CHANGELOGS.tap_stream_id):
            changelogs_to_write = []
            issue_id = str(issue["id"])  # Ensure consistent string type
            
            # Use bulk-fetched changelog data
            if changelog_map is not None and issue_id in changelog_map:
                changelogs_to_write = changelog_map[issue_id]
                # Ensure issueId is set on each changelog
                for changelog in changelogs_to_write:
                    changelog["issueId"] = issue_id
            elif changelog_map is not None:
                # No changelogs found for this issue (empty list)
                changelogs_to_write = []
            else:
                # This should not happen since we always bulk fetch if changelogs are selected
                raise Exception(f"Changelog map is None but changelogs are selected for issue {issue_id}")


            for changelog in changelogs_to_write:
                changelog_items = []
                for item in changelog['items']:
                    if 'fieldId' in item and should_exclude_field(item['fieldId'], item['field']):
                        if 'from' in item and item['from'] is not None and item['from'] != '':
                            item['from'] = '<REDACTED>'
                        if 'fromString' in item and item['fromString'] is not None and item['fromString'] != '':
                            item['fromString'] = '<REDACTED>'
                        if 'to' in item and item['to'] is not None and item['to'] != '':
                            item['to'] = '<REDACTED>'
                        if 'toString' in item and item['toString'] is not None and item['toString'] != '':
                            item['toString'] = '<REDACTED>'
                        
                    changelog_items.append(item)
                changelog['items'] = changelog_items

            CHANGELOGS.write_page(
                [{ **changelog, 'issueId': issue["id"] } for changelog in changelogs_to_write]
            )
            
            # Monitor memory after processing large changelog sets
            if len(changelogs_to_write) > 100:
                log_memory("Memory after %d changelogs for issue %s", 
                          len(changelogs_to_write), issue["id"])

        # Note: Transitions are not available via expand in API v3
        # We will need to fetch them separately if they are needed
        # These are just the transitions that are available for the issue
        # And we don't need them for the current use case


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
                result = self._sync_project_with_error_handling(fieldNames, knownFields, None)
                if result["status"] != "success":
                    LOGGER.warning(f"Project sync for ALL_PROJECTS completed with status: {result['status']}")
        else:
            # Process projects sequentially to avoid rate limit quota bursts and connection pool exhaustion
            success_count = 0
            error_count = 0
            failed_projects = []
            
            for project_key_or_id in projectsToSync:
                try:
                    result = self._sync_project_with_error_handling(fieldNames, knownFields, project_key_or_id)
                    if result["status"] == "success":
                        success_count += 1
                    else:
                        # This should only happen for handled 400 errors
                        error_count += 1
                        failed_projects.append(result["project"])
                except Exception as exc:
                    # This will happen for any re-raised exceptions from _sync_project_with_error_handling
                    # We need to re-raise to ensure the tap fails properly
                    LOGGER.error(f"A project sync failed with an unhandled error: {exc}")
                    raise exc
            
            LOGGER.info(f"Completed processing {len(projectsToSync)} projects:")
            LOGGER.info(f"  - {success_count} succeeded")
            if error_count > 0:
                LOGGER.warning(f"  - {error_count} failed with handled errors (these projects were skipped)")
                LOGGER.warning(f"Failed projects: {', '.join(failed_projects)}")

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

    def _sync_project_with_error_handling(self, fieldNames, knownFields, project_key_or_id):
        """Wrapper method to handle errors during project sync in threads"""
        try:
            LOGGER.info(f"Starting sync for project: {project_key_or_id}")
            with Timer('issues_sync', { 'project': project_key_or_id }):
                self.sync_project(fieldNames, knownFields, project_key_or_id)
            LOGGER.info(f"Successfully completed sync for project: {project_key_or_id}")
            return {"status": "success", "project": project_key_or_id}
        except requests.exceptions.HTTPError as http_err:
            # Handle specific 400 errors at the project level
            if http_err.response.status_code == 400 and '/rest/api/3/search/jql' in http_err.response.url:
                LOGGER.warning(f"Project {project_key_or_id}: Encountered a handled 400 error with Jira search API")
                LOGGER.warning(f"URL: {http_err.response.url}")
                LOGGER.warning(f"Response body: {http_err.response.text}")
                LOGGER.warning(f"This error is being handled as non-fatal. Sync will continue with other projects.")
                return {"status": "error", "project": project_key_or_id, "error_type": "handled_400"}
            else:
                # Re-raise other HTTP errors
                LOGGER.error(f"Project {project_key_or_id}: Encountered an HTTP error: {http_err}")
                raise http_err
        except Exception as exc:
            # Re-raise other exceptions
            LOGGER.error(f"Project {project_key_or_id}: Encountered an error: {exc}")
            raise exc

    def sync_project(self, fieldNames, knownFields, project_key_or_id = None):
        if project_key_or_id is None:
            project_key_or_id = self.ALL_PROJECTS_BOOKMARK_KEY

        LOGGER.info('Begin syncing issues for project {}'.format(project_key_or_id))
        
        # Monitor memory at start of project sync
        log_memory("Starting memory for project %s", project_key_or_id)

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

        # Two-phase approach for optimal performance
        jql = "{} updated >= '{}' order by updated asc".format(projectsJql, start_date).strip()
        
        # Phase 1: Get ALL issue IDs in chronological order (fast, guaranteed ordering)
        # JQL "ORDER BY updated ASC" ensures IDs are returned in chronological sequence
        LOGGER.info("Phase 1: Fetching all issue IDs in chronological order for project %s", project_key_or_id)
        id_pager = EnhancedSearchPaginator(Context.client, max_results=5000, ids_only=True)
        all_issue_ids = []  # This will contain IDs in chronological order by 'updated' timestamp
        
        for id_page in id_pager.pages(self.tap_stream_id, jql):
            issue_ids = [issue["id"] for issue in id_page]
            all_issue_ids.extend(issue_ids)  # Preserves chronological order from JQL
            LOGGER.info("Collected %d issue IDs (total: %d)", len(issue_ids), len(all_issue_ids))
        
        LOGGER.info("Phase 1 complete: Found %d total issues in chronological order for project %s", len(all_issue_ids), project_key_or_id)
        
        if not all_issue_ids:
            LOGGER.info("No issues found for project %s", project_key_or_id)
            return
        
        # Phase 2: Process ordered IDs in batches using bulk fetch (preserves order from Phase 1)
        LOGGER.info("Phase 2: Processing %d ordered issue IDs in batches", len(all_issue_ids))
        batch_size = 5000  # Bulk fetch batch size
        sub_batch_size = 100  # Writing sub-batch size
        max_updated_seen = None
        
        for batch_start in range(0, len(all_issue_ids), batch_size):
            batch_end = min(batch_start + batch_size, len(all_issue_ids))
            batch_ids = all_issue_ids[batch_start:batch_end]  # Maintains JQL order!
            
            batch_index = batch_start // batch_size
            LOGGER.info("Processing batch %d: issues %d-%d (%d issues) for project %s", 
                       batch_index, batch_start, batch_end-1, len(batch_ids), project_key_or_id)
            
            # Monitor memory at start of batch
            log_memory("Memory before processing batch %d for project %s", batch_index, project_key_or_id)
            
            # Bulk fetch issues for this batch (parallel execution scrambles the order)
            batch_issues = bulk_fetch_issues_parallel(Context.client, self.tap_stream_id, batch_ids, ["*all"])
            
            # CRITICAL: Restore chronological order that was scrambled by parallel bulk fetch
            # - batch_ids contains IDs in JQL chronological order (ORDER BY updated ASC)
            # - batch_issues contains the same issues but in random order due to parallel execution
            # - We use issue IDs as lookup keys to rebuild the chronological sequence
            LOGGER.info("Restoring chronological order for %d bulk-fetched issues", len(batch_issues))
            id_to_issue = {issue["id"]: issue for issue in batch_issues}  # Create lookup map
            ordered_batch_issues = []
            for issue_id in batch_ids:  # Iterate in original JQL chronological order
                if issue_id in id_to_issue:
                    # Add issue back in chronological position (not sorted by ID, but by updated timestamp)
                    ordered_batch_issues.append(id_to_issue[issue_id])
                else:
                    LOGGER.warning("Issue ID %s not found in bulk fetch results", issue_id)
            
            LOGGER.info("Batch %d: Restored chronological order for %d issues", batch_index, len(ordered_batch_issues))
            
            # Fetch changelogs for this batch if needed
            changelog_map = None
            if Context.is_selected(CHANGELOGS.tap_stream_id):
                LOGGER.info("Fetching changelogs for batch %d (%d issues)", batch_index, len(batch_ids))
                changelog_map = bulk_fetch_changelogs_for_issues(Context.client, CHANGELOGS.tap_stream_id, batch_ids)
            
            # Process batch in smaller sub-batches for writing (maintaining JQL order)
            for sub_batch_index, sub_batch_start in enumerate(range(0, len(ordered_batch_issues), sub_batch_size)):
                sub_batch_end = min(sub_batch_start + sub_batch_size, len(ordered_batch_issues))
                issue_batch = ordered_batch_issues[sub_batch_start:sub_batch_end]
                
                LOGGER.info("Processing sub-batch %d (%d-%d) with %d issues from batch %d", 
                           sub_batch_index, sub_batch_start, sub_batch_end-1, len(issue_batch), batch_index)
                
                # sync comments and changelogs for each issue
                sync_sub_streams(issue_batch, issue_changelogs_updated, changelog_map)
                
                for issue in issue_batch:
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

                    # Track maximum updated timestamp seen - in JQL order, this will be monotonically increasing
                    issue_updated = utils.strptime_to_utc(issue["fields"]["updated"])
                    if max_updated_seen is None or issue_updated > max_updated_seen:
                        max_updated_seen = issue_updated

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
                    
                LOGGER.info("Writing sub-batch %d with %d issues from batch %d...", sub_batch_index, len(issue_batch), batch_index)
                with self.write_lock:
                    self.write_page(issue_batch)
                    # Update bookmark with maximum updated timestamp (safe since issues are JQL-ordered)
                    if max_updated_seen:
                        Context.set_bookmark(updated_bookmark, max_updated_seen)
                    Context.set_bookmark(issue_changelogs_updated_bookmark_path, issue_changelogs_sync_time)
                    singer.write_state(Context.state)
                
                # Monitor memory usage after processing each batch
                log_memory("Memory after sub-batch %d from batch %d", sub_batch_index, batch_index)
                
                LOGGER.info("Finished writing sub-batch %d from batch %d", sub_batch_index, batch_index)
            
            # Batch complete - clear variables to help with memory cleanup
            log_memory("Memory at end of batch %d (should decrease on next batch)", batch_index)
            del ordered_batch_issues
            if changelog_map:
                del changelog_map
        
        # All batches complete - final state update
        LOGGER.info("All ordered batches complete for project %s", project_key_or_id)
        with self.write_lock:
            # Final bookmark update with maximum updated timestamp seen
            if max_updated_seen:
                Context.set_bookmark(updated_bookmark, max_updated_seen)
                LOGGER.info("Final bookmark set to maximum updated timestamp: %s", max_updated_seen)
            Context.set_bookmark(issue_changelogs_updated_bookmark_path, issue_changelogs_sync_time)
            singer.write_state(Context.state)

        # Final memory report for this project
        log_memory("Final memory after syncing project %s", project_key_or_id)
        
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
