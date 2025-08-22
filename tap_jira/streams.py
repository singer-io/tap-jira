import json
import pytz
import singer
import dateparser
import concurrent.futures
from typing import Dict, List, Any, Set

from singer import metrics, utils, metadata, Transformer
from singer.transform import SchemaMismatch
from dateutil.parser._parser import ParserError
from requests.exceptions import RequestException
from .http import Paginator, CursorPaginator, JiraNotFoundError
from .context import Context
from .jira_utils.flatten_description import flatten_description

DEFAULT_PAGE_SIZE = 50

def handle_date_time_schema_mis_match(exception, record, pk_fields): # pylint: disable=inconsistent-return-statements
    """
    Handling exception for date-time value out of range.
    """

    if ("{'format': 'date-time', 'type': ['string', 'null']}"
        in exception.args[0].split("\n\t")[1]):
        nested_keys = exception.args[0].split("\n\t")[1].split(":")[0]

        obj = record.copy()
        # Getting the date value from nested object
        for key in nested_keys.split("."):
            # Convert list index of nested object to integer
            obj = obj[int(key) if key.isnumeric() else key]

        try:
            # Parsing date to catch 'out of range' error
            utils.strptime_to_utc(obj)
        except ParserError as err:

            # Check the error message if the 'year' or 'day' is out of range
            # For Example: year 51502 is out of range: 51502-06-08T14:46:42.000000
            if ("out of range" in str(err)) or (
                # Check the error message if 'month' or ['hours','minutes','seconds'] given in date is not in range
                # example: month must be in 1..12: 5150-33-08T14:46:42.000000
                "must be in" in str(err)):
                LOGGER.warning("Skipping record of: %s due to Date out of range, DATE: %s", dict((pk, record.get(pk)) for pk in pk_fields), obj)
                return True
            else:
                # Raise an error for exception except 'out of range' date
                raise err
    else:
        # Raise a schema mismatch error, other than date out of range values
        raise exception

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
        rec_count = 0
        stream = Context.get_catalog_entry(self.tap_stream_id)
        stream_metadata = metadata.to_map(stream.metadata)
        extraction_time = singer.utils.now()
        for rec in page:
            with Transformer() as transformer:
                try:
                    rec = transformer.transform(rec, stream.schema.to_dict(), stream_metadata)
                except SchemaMismatch as ex:
                    # Checking if schema-mismatch is occurring for datetime value
                    # TDL-19174: Transformation issue for "date out of range"
                    if handle_date_time_schema_mis_match(ex, rec, self.pk_fields):
                        continue    # skipping record for this error
            singer.write_record(self.tap_stream_id, rec, time_extracted=extraction_time)
            rec_count += 1 # increment counter only after the record is written

        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(rec_count) # Do not increment counter for skipped records

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
            params={"expand": "description,lead,url,projectKeys,permissions"})
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
                # For simplified projects, we want to know if the project is public or not. Non simplified projects
                # are handled at company-level, and will have explicit permissions, through groups and users (see: roles)
                # The only way to know the access level of a simplified project is a non-public API, this is dirty but
                # we don't have a choice. Other name for simplified projects is "team-managed projects"
                # Assume private by default, for safety concerns. Possible values are PRIVATE, LIMITED, OPEN and FREE
                access_level = "PRIVATE"
                if project.get("simplified"):
                    try:
                        access_level_req = Context.client.request_internal(
                                self.tap_stream_id,
                                "GET",
                                # Jira internal API, may change at any time - keep an eye on this
                                f"/rest/internal/simplified/1.0/accesslevel/project/{project.get('id')}",
                                {
                                    'accept': 'application/json,text/javascript,*/*',
                                    'content-type': 'application/json',
                                }
                            )
                        LOGGER.info(f"Access level for project {project.get('id')}: {access_level_req}")
                        if isinstance(access_level_req, dict):
                            # For traditional projects, an error is returned as a string (local translated)
                            access_level = access_level_req.get("value")
                    except RequestException as e:
                        # If the request fails, we assume the project is private
                        LOGGER.warning(f"Failed to get access level for project {project.get('id')}: {e}")
                project["accessLevel"] = access_level

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

        # Fetch the groups dynamically
        groups = []
        pager = Paginator(Context.client, items_key='values')
        for page in pager.pages(self.tap_stream_id, "GET",
                                        "/rest/api/2/group/bulk"):
            for grp in page:
                groups.append(grp["name"])

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

class Groups(Stream):
    def sync(self):
        groups = []
        pager = Paginator(Context.client, items_key='values')
        for grp_page in pager.pages(self.tap_stream_id, "GET", "/rest/api/2/group/bulk"):
            for grp in grp_page:
                group_name = grp["name"]
                users = []
                try:
                    params = {
                        "groupname": group_name,
                        "maxResults": 50, 
                        "includeInactiveUsers": False
                    }
                    pager = Paginator(Context.client, items_key='values')
                    for usr_page in pager.pages(self.tap_stream_id, "GET","/rest/api/2/group/member", params=params):
                        for usr_data in usr_page:
                            # Remove a bit of bloat
                            usr_data.pop("avatarUrls", None)
                            usr_data.pop("expand", None)
                            users.append(usr_data)
                except JiraNotFoundError:
                    LOGGER.info("Could not find group \"%s\", skipping", group_name)
                except Exception as e:
                    LOGGER.warning("Failed to fetch members for group \"%s\": %s", group_name, str(e))
                grp["id"] = grp["groupId"]
                grp["users"] = users
                groups.append(grp)

                extraction_time = singer.utils.now()
                singer.write_record(self.tap_stream_id, grp, time_extracted=extraction_time)


class Roles(Stream):
    # Class-level cache for group members: groups are likely to be common across projects
    _group_members_cache: Dict[str, List[Dict[str, Any]]] = {}
    
    def __init__(self, tap_stream_id, pk_fields, indirect_stream=False, max_workers: int = 10):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields
        # Only used to skip streams in the main sync function
        self.indirect_stream = indirect_stream
        # Number of worker threads for parallel processing
        self.max_workers = max_workers

    def get_group_members(self, group_name: str) -> List[Dict[str, Any]]:
        """Fetch members for a specific group with caching"""
        # Return from cache if available
        if group_name in self._group_members_cache:
            LOGGER.debug(f"Using cached members for group {group_name}")
            return self._group_members_cache[group_name]
            
        LOGGER.debug(f"Fetching members for group {group_name}")
        group_members = []
        try:
            start_at = 0
            max_results = 50
            while True:
                # Request group members
                members_response = Context.client.request(
                    self.tap_stream_id,
                    "GET",
                    f"/rest/api/3/group/member",
                    params={
                        "groupname": group_name,
                        "startAt": start_at,
                        "maxResults": max_results
                    }
                )
                
                # Process members from response
                if "values" in members_response:
                    for member in members_response["values"]:
                        group_members.append({
                            "account_id": member.get("accountId"),
                            "display_name": member.get("displayName"),
                            "active": member.get("active"),
                            "email_address": member.get("emailAddress")
                        })
                
                # Check if we need to fetch more members
                if not members_response.get("isLast", True):
                    start_at += max_results
                else:
                    break
                    
        except Exception as e:
            LOGGER.warning(f"Failed to fetch members for group {group_name}: {str(e)}")
        
        # Store in cache for future use
        self._group_members_cache[group_name] = group_members
        return group_members

    def get_project_roles(self, project: Dict[str, str]) -> List[Dict[str, Any]]:
        """Process all roles for a single project"""
        project_roles = []
        try:
            roles_response = Context.client.request(
                self.tap_stream_id,
                "GET",
                f"/rest/api/2/project/{project['key']}/role"
            )
            
            for role_name, role_url in roles_response.items():
                role_details = self.process_role(project, role_name, role_url)
                if role_details:
                    project_roles.append(role_details)
                    
        except JiraNotFoundError:
            LOGGER.info(f"Could not find project \"{project['key']}\", skipping")
        except Exception as e:
            LOGGER.error(f"Error processing project {project['key']}: {str(e)}")
            
        return project_roles

    def process_role(self, project: Dict[str, str], role_name: str, role_url: str) -> Dict[str, Any]:
        """Process a single role for a project"""
        try:
            role_id = role_url.split('/')[-1]
            role_details = Context.client.request(
                self.tap_stream_id,
                "GET", 
                f"/rest/api/2/project/{project['key']}/role/{role_id}"
            )
            
            # Enhance the role details with project information
            role_details["project_id"] = project["id"]
            role_details["project_key"] = project["key"]
            role_details["project_name"] = project["name"]
            role_details["role_name"] = role_name
            
            # Process actors separately (users and groups)
            users = []
            groups = []
            group_names = set()
            
            if "actors" in role_details:
                for actor in role_details["actors"]:
                    if actor["type"] == "atlassian-user-role-actor":
                        users.append({
                            "id": actor.get("actorUser", {}).get("accountId"),
                            "name": actor.get("displayName"),
                            "email": actor.get("actorUser", {}).get("emailAddress")
                        })
                    elif actor["type"] == "atlassian-group-role-actor":
                        group_name = actor.get("displayName")
                        if group_name:
                            group_names.add(group_name)
            
            # Placeholder for groups with members - to be filled later with parallel processing
            role_details["users"] = users
            role_details["groups"] = [{"name": name, "members": []} for name in group_names]
            role_details["group_names"] = list(group_names)
            
            return role_details
            
        except Exception as e:
            LOGGER.error(f"Error processing role {role_name} for project {project['key']}: {str(e)}")
            return None

    def sync(self):
        # First get all projects
        offset = 0
        projects = []
        while True:
            params = {
                "expand": "description,lead,url,projectKeys",
                "maxResults": DEFAULT_PAGE_SIZE,
                "startAt": offset
            }
            projects_data = Context.client.request(
                self.tap_stream_id, "GET", "/rest/api/3/project/search",
                params=params)
            for project in projects_data.get('values'):
                projects.append({
                    "id": project["id"],
                    "key": project["key"],
                    "name": project["name"],
                    "simplified": project.get("simplified"),
                    "style": project.get("style"),
                })
            if projects_data.get("isLast"):
                break
            offset = offset + DEFAULT_PAGE_SIZE
        
        all_roles_data = []
        all_groups = set()
        
        LOGGER.info(f"Processing roles for {len(projects)} projects in parallel")
        # Base concept of what we want to retrieve: each project has a set of roles which
        # can be shared across projects. We wish to retrieve access info: which users have
        # direct access to projects, or indirect access via groups.
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_project = {
                executor.submit(self.get_project_roles, project): project 
                for project in projects
            }
            
            for future in concurrent.futures.as_completed(future_to_project):
                project = future_to_project[future]
                try:
                    project_roles = future.result()
                    for role in project_roles:
                        all_roles_data.append(role)
                        all_groups.update(role.get("group_names", []))
                except Exception as e:
                    LOGGER.error(f"Failed to process roles for project {project['key']}: {str(e)}")
        
        # Fetch all group members in parallel
        LOGGER.info(f"Fetching members for {len(all_groups)} unique groups")
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Start the operations and mark each future with its group name
            future_to_group = {
                executor.submit(self.get_group_members, group_name): group_name 
                for group_name in all_groups
            }
            
            # Process results as they become available
            for future in concurrent.futures.as_completed(future_to_group):
                group_name = future_to_group[future]
                try:
                    # Result is already stored in cache by get_group_members
                    future.result()
                except Exception as e:
                    LOGGER.error(f"Error fetching members for group {group_name}: {str(e)}")
        
        # Now write all roles with their group members
        for role_details in all_roles_data:
            # Populate the groups with their members from cache
            if "groups" in role_details:
                for group in role_details["groups"]:
                    group_name = group["name"]
                    group["members"] = self._group_members_cache.get(group_name, [])
            
            # Remove the temporary group_names field
            if "group_names" in role_details:
                del role_details["group_names"]
                
            extraction_time = singer.utils.now()
            singer.write_record(self.tap_stream_id, role_details, time_extracted=extraction_time)


class Issues(Stream):

    def sync(self):
        updated_bookmark = [self.tap_stream_id, "updated"]
        cursor_bookmark = [self.tap_stream_id, "cursor", "next_page_token"]

        last_updated = Context.update_start_date_bookmark(updated_bookmark)
        timezone = Context.retrieve_timezone()
        start_date = last_updated.astimezone(pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M")

        jql = "updated >= '{}' order by updated asc".format(start_date)
        params = {
            "jql": jql,
            "fields": ["*all"],
            "expand": "changelog,transitions",
            "maxResults": DEFAULT_PAGE_SIZE,
        }

        # Use CursorPaginator for v3/search/jql endpoint
        pager = CursorPaginator(Context.client, items_key="issues")

        # Restore cursor state if available
        saved_cursor = Context.bookmark(cursor_bookmark) or None
        if saved_cursor:
            pager.next_page_token = saved_cursor

        for page in pager.pages(
            self.tap_stream_id, "GET", "/rest/api/3/search/jql", params=params
        ):
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
                if "description" in issue["fields"] and issue["fields"]["description"]:
                    try:
                        issue["fields"]["description"] = flatten_description(issue["fields"]["description"])
                    except Exception as e:
                        # Log the error and keep original description
                        print(f"Error flattening description: {e}")
                        raise Exception(f"Failed to flatten description: {e}")

            # Grab last_updated before transform in write_page
            last_updated = utils.strptime_to_utc(page[-1]["fields"]["updated"])

            self.write_page(page)

            # Save cursor state for resumption
            Context.set_bookmark(cursor_bookmark, pager.next_page_token)
            singer.write_state(Context.state)

        # Clear cursor bookmark when sync is complete
        Context.set_bookmark(cursor_bookmark, None)
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
    Stream("fields", ["id"], path="/rest/api/3/field"),
    PROJECTS,
    VERSIONS,
    COMPONENTS,
    ProjectTypes("project_types", ["key"]),
    Stream("project_categories", ["id"], path="/rest/api/2/projectCategory"),
    Stream("resolutions", ["id"], path="/rest/api/2/resolution"),
    Roles("roles", ["id"]),
    Users("users", ["accountId"]),
    Groups("groups", ["id"]),
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
