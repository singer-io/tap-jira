import json
import string
import pytz
import requests
import singer
from singer import utils

from .utils import advance_bookmark, deep_get


LOGGER = singer.get_logger()


class Stream:
    endpoint = None
    paginate = False
    params = None

    tap_stream_id = None
    key_properties = None
    replication_key = "fields.updated"
    replication_method = "FULL_TABLE"

    def __repr__(self):
        return "<Stream(" + self.tap_stream_id + ")>"

    def update_replication_method(self, method):
        if method:
            self.replication_method = method

    def sync(self, client, config, state, **kwargs) -> (any, int):
        if self.paginate:
            yield from client.fetch_pages(
                self.tap_stream_id, self.endpoint, params=self.params)
        else:
            yield client.get(self.tap_stream_id, self.endpoint, params=self.params), 0


class Boards(Stream):
    endpoint = "/rest/agile/1.0/board"
    paginate = True
    tap_stream_id = "boards"
    key_properties = ["id"]


class IssueBoard(Stream):
    endpoint = "/rest/agile/1.0/board/{}/issue"
    paginate = True
    params = {"expand": "sprint,epic,project,created,updated"}
    tap_stream_id = "issue_board"
    key_properties = ["id"]

    def sync(self, client, config, state, **kwargs):
        record = kwargs.get('record', {})
        fendpoint = self.endpoint.format(record['id'])

        # custom state handling to keep track of board issues
        bookmark_id = str(record['id'])
        bookmark = singer.get_bookmark(
            state, self.tap_stream_id, bookmark_id, {})

        offset = 0
        start_dt = bookmark.get(self.replication_key, config['start_date'])
        start_dt = utils.strptime_to_utc(start_dt)

        timezone = config.get('timezone', 'UTC')
        start_date = start_dt.astimezone(
            pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M")

        self.params['jql'] = "updated >= '{}' order by updated asc".format(
            start_date)

        max_updated = start_dt
        for page, cursor in client.fetch_pages(
                self.tap_stream_id, fendpoint,
                items_key="issues",
                startAt=offset,
                params=self.params
        ):
            for entry in page:
                entry['boardId'] = record['id']
                updated_at = deep_get(entry, self.replication_key)
                updated_at = utils.strptime_to_utc(updated_at)
                if updated_at and updated_at > max_updated:
                    max_updated = updated_at

            state = singer.write_bookmark(state, self.tap_stream_id, bookmark_id, {
                self.replication_key: max_updated.strftime(
                    utils.DATETIME_PARSE)
            })

            singer.write_state(state)
            yield page, cursor


class ProjectBoard(Stream):
    endpoint = "/rest/agile/1.0/board/{}/project"
    paginate = True
    tap_stream_id = "project_board"
    key_properties = ["id"]

    def sync(self, client, config, state, **kwargs):
        record = kwargs.get('record', {})
        fendpoint = self.endpoint.format(record['id'])
        for page, cursor in client.fetch_pages(self.tap_stream_id, fendpoint):
            for entry in page:
                entry['boardId'] = record['id']
            yield page, cursor


class Epics(Stream):
    endpoint = "/rest/agile/1.0/board/{}/epic"
    paginate = True
    tap_stream_id = "epics"
    key_properties = ["id"]

    def sync(self, client, config, state, **kwargs):
        record = kwargs.get('record', {})
        fendpoint = self.endpoint.format(record['id'])
        for page, cursor in client.fetch_pages(self.tap_stream_id, fendpoint):
            for entry in page:
                entry['boardId'] = record['id']
            yield page, cursor


class Sprints(Stream):
    endpoint = "/rest/agile/1.0/board/{}/sprint"
    paginate = True
    tap_stream_id = "sprints"
    key_properties = ["id"]

    def sync(self, client, config, state, **kwargs):
        record = kwargs.get('record', {})
        fendpoint = self.endpoint.format(record['id'])
        try:
            for page, cursor in client.fetch_pages(
                    self.tap_stream_id, fendpoint):
                for entry in page:
                    entry['boardId'] = record['id']
                yield page, cursor
        # Not every board supports sprints
        except requests.exceptions.HTTPError as http_error:
            if http_error.response.status_code == 400:
                LOGGER.info(
                    "Could not find sprint for board \"%s\", skipping", record['id'])
                yield [], 0
            else:
                raise http_error


class Projects(Stream):
    endpoint = "/rest/api/2/project"
    params = {"expand": "description,lead,url,projectKeys"}
    tap_stream_id = "projects"
    key_properties = ["id"]

    def sync(self, client, config, state, **kwargs):
        projects = client.get(self.tap_stream_id, self.endpoint)
        for project in projects:
            project.pop("versions", None)

        yield projects, 0


class ProjectTypes(Stream):
    endpoint = "/rest/api/2/project/type"
    tap_stream_id = "project_types"
    key_properties = ["key"]

    def sync(self, client, config, state, **kwargs):
        types = client.get(self.tap_stream_id, self.endpoint)
        for typ in types:
            typ.pop("icon", None)

        yield types, 0


class ProjectCategories(Stream):
    endpoint = "/rest/api/2/projectCategory"
    tap_stream_id = "project_categories"
    key_properties = ["id"]


class Versions(Stream):
    endpoint = "/rest/api/2/project/{}/version"
    params = {"orderBy": "sequence"}
    tap_stream_id = "versions"
    key_properties = ["id"]

    def sync(self, client, config, state, **kwargs):
        record = kwargs.get('record', {})
        fendpoint = self.endpoint.format(record['id'])
        yield from client.fetch_pages(self.tap_stream_id, fendpoint)


class Resolutions(Stream):
    endpoint = "/rest/api/2/resolution"
    tap_stream_id = "resolutions"
    key_properties = ["id"]


class Roles(Stream):
    endpoint = "/rest/api/2/role"
    tap_stream_id = "roles"
    key_properties = ["id"]


class Users(Stream):
    endpoint = "/rest/api/2/group/member"
    tap_stream_id = "users"
    key_properties = ["accountId"]
    groups = ["jira-administrators",
              "jira-software-users",
              "jira-core-users",
              "jira-users",
              "uses"]

    def sync(self, client, config, state, **kwargs):
        groups = config.get("groups")
        if not groups:
            groups = self.groups

        for group in groups:
            params = {
                "groupname": group,
                "maxRersults": 2,
                "includeInactiveUsers": True
            }
            try:
                for page, cursor in client.fetch_pages(
                        self.tap_stream_id, self.endpoint, params=params):
                    yield page, cursor
            except requests.exceptions.HTTPError as http_error:
                if http_error.response.status_code == 404:
                    LOGGER.info("Could not find group \"%s\", skipping", group)
                    yield [], 0
                else:
                    raise http_error


class Issues(Stream):
    endpoint = "/rest/api/2/search"
    paginate = True
    tap_stream_id = "issues"
    key_properties = ["id"]

    def get_issue_field_map(self, client):
        result = {}
        endpoint = "/rest/api/3/field"
        fields = client.get(self.tap_stream_id, endpoint)
        translator = str.maketrans('', '', string.punctuation)
        for field in fields:
            key = field["id"]
            # strip punctuations from custom field names
            name = field["name"].translate(translator)
            # snake case field names
            name = name.replace(" ", "_")
            result[key] = name.lower()
        return result

    def rename_fields(self, record, mapper):
        fields = record['fields'].copy()
        renamed_fields = {mapper.get(k, k): v for k, v in fields.items()}
        record['fields'] = renamed_fields
        return record

    def sync(self, client, config, state, **kwargs):
        offset = kwargs.get('offset')
        start_date = kwargs.get('start_date')
        start_date = utils.strptime_to_utc(start_date)
        timezone = config.get('timezone', 'UTC')
        start_date = start_date.astimezone(
            pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M")

        jql = "updated >= '{}' order by updated asc".format(start_date)
        params = {"fields": "*all",
                  "expand": "changelog,transitions",
                  "validateQuery": "strict",
                  "jql": jql}

        field_names = self.get_issue_field_map(client)
        for page, cursor in client.fetch_pages(
                self.tap_stream_id,
                self.endpoint,
                items_key="issues",
                startAt=offset,
                params=params
        ):
            # New page contains all issues for a page
            # with renamed custom fields
            new_page = [self.rename_fields(r, field_names) for r in page]
            yield new_page, cursor


class IssueComments(Stream):
    tap_stream_id = "issue_comments"
    key_properties = ["id"]

    def sync(self, client, config, state, **kwargs):
        record = kwargs.get('record', {})
        comments = record.get('fields', {}).pop(
            'comment', {}).get('comments', [])
        for comment in comments:
            comment["issueId"] = record["id"]

        yield comments, 0


class IssueTransitions(Stream):
    tap_stream_id = "issue_transitions"
    key_properties = ["id"]

    def sync(self, client, config, state, **kwargs):
        record = kwargs.get('record', {})
        transitions = record.pop("transitions", [])
        for transition in transitions:
            transition["issueId"] = record["id"]

        yield transitions, 0


class IssueChangelogs(Stream):
    tap_stream_id = "issue_changelogs"
    key_properties = ["id"]

    def sync(self, client, config, state, **kwargs):
        record = kwargs.get('record', {})
        changelogs = record.pop("changelog", {}).get('histories', [])
        for changelog in changelogs:
            changelog["issueId"] = record["id"]

        yield changelogs, 0


class Worklogs(Stream):
    endpoint = "/rest/api/2/worklog/list"
    tap_stream_id = "worklogs"
    key_properties = ["id"]
    replication_key = "updated"

    def _fetch_ids(self, client, last_updated):
        # since_ts uses millisecond precision
        since_ts = int(last_updated.timestamp()) * 1000
        endpoint = "/rest/api/2/worklog/updated"
        return client.get(self.tap_stream_id, endpoint,
                          params={"since": since_ts})

    def _fetch_worklogs(self, client, ids):
        if not ids:
            return []
        return client.request(
            self.tap_stream_id, "POST", "/rest/api/2/worklog/list",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"ids": ids}),
        )

    def sync(self, client, config, state, **kwargs):
        last_updated = kwargs.get('start_date')
        last_updated = utils.strptime_to_utc(last_updated)

        while True:
            ids_page = self._fetch_ids(client, last_updated)
            if not ids_page["values"]:
                break
            ids = [x["worklogId"] for x in ids_page["values"]]
            worklogs = self._fetch_worklogs(client, ids)

            # Grab last_updated before transform in write_page
            last_updated = advance_bookmark(worklogs)
            state = singer.write_bookmark(
                state, self.tap_stream_id, self.replication_key, utils.strftime(last_updated))
            singer.write_state(state)

            yield worklogs, 0

            # lastPage is a boolean value based on
            # https://developer.atlassian.com/cloud/jira/platform/rest/v3/?utm_source=%2Fcloud%2Fjira%2Fplatform%2Frest%2F&utm_medium=302#api-api-3-worklog-updated-get
            last_page = ids_page.get("lastPage")
            if last_page:
                break


STREAMS = {
    'boards': {
        'cls': Boards,
        'substreams': {
            'issue_board': IssueBoard,
            'project_board': ProjectBoard,
            'epics': Epics,
            'sprints': Sprints
        }
    },
    'issues': {
        'cls': Issues,
        'substreams': {
            'issue_comments': IssueComments,
            'issue_transitions': IssueTransitions,
            'issue_changelogs': IssueChangelogs,
        }
    },
    'projects': {
        'cls': Projects,
        'substreams': {
            'versions': Versions
        }
    },
    'project_types': ProjectTypes,
    'project_categories': ProjectCategories,
    'roles': Roles,
    'users': Users,
    'resolutions': Resolutions,
    'worklogs': Worklogs
}
