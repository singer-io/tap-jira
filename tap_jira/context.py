from datetime import datetime
from singer import utils, metadata
from functools import cache

class Context():
    config = None
    state = None
    catalog = None
    client = None
    stream_map = {}
    allProjectsList = []

    @classmethod
    def get_projects(cls):
        projectsRaw = cls.config.get("projects", None)
        excludeProjects = cls.config.get("exclude_projects", None)
        if projectsRaw:
            # convert comma-separated list into array
            projectsList = list(map(lambda p: p.strip(), projectsRaw.split(",")))
            # filter out empty strings
            projectsList = list(filter(lambda p: len(p) > 0, projectsList))
            return projectsList
        elif excludeProjects:
            # convert comma-separated list into array
            excludeProjectsList = list(map(lambda p: p.strip(), excludeProjects.split(",")))
            # filter out empty strings
            excludeProjectsList = list(filter(lambda p: len(p) > 0, excludeProjectsList))
            return list(set(cls.allProjectsList) - set(excludeProjectsList))
        else:
            return cls.allProjectsList

    @classmethod
    def set_available_projects(cls, projectList):
        projectKeys = []
        for project in projectList:
            projectKeys.append(project["key"])
        cls.allProjectsList = projectKeys

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s.tap_stream_id: s for s in cls.catalog.streams}
        return cls.stream_map.get(stream_name)

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        if stream is None:
            return False
        stream_metadata = metadata.to_map(stream.metadata)
        return metadata.get(stream_metadata, (), 'selected')

    @classmethod
    def bookmarks(cls):
        if "bookmarks" not in cls.state:
            cls.state["bookmarks"] = {}
        return cls.state["bookmarks"]

    @classmethod
    def bookmark(cls, paths):
        bookmark = cls.bookmarks()
        for path in paths:
            if path not in bookmark:
                bookmark[path] = {}
            bookmark = bookmark[path]
        return bookmark

    @classmethod
    def set_bookmark(cls, path, val):
        if isinstance(val, datetime):
            val = utils.strftime(val)

        if val is None:
            cls.bookmark(path[:-1]).pop(path[-1], None)
        else:
            cls.bookmark(path[:-1])[path[-1]] = val

    @classmethod
    def update_start_date_bookmark(cls, path):
        val = cls.bookmark(path)
        if not val:
            val = cls.config["start_date"]
            val = utils.strptime_to_utc(val)
            cls.set_bookmark(path, val)
        if isinstance(val, str):
            val = utils.strptime_to_utc(val)
        return val

    @classmethod
    @cache
    def retrieve_timezone(cls):
        response_json = cls.client.request("timezone", "GET", "/rest/api/2/myself")
        return response_json["timeZone"]

    @classmethod
    def get_exclude_issue_fields(cls):
        return [str(x) for x in cls.config.get("exclude_issue_fields", [])]
