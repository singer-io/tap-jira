#!/usr/bin/env python3
import os
import json
import singer
from singer import utils
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams as streams_
from .context import Context
from .http import Client, JiraForbiddenError

LOGGER = singer.get_logger()
REQUIRED_CONFIG_KEYS_CLOUD = ["start_date",
                              "user_agent",
                              "cloud_id",
                              "access_token",
                              "refresh_token",
                              "oauth_client_id",
                              "oauth_client_secret"]
REQUIRED_CONFIG_KEYS_HOSTED = ["start_date",
                               "username",
                               "password",
                               "base_url",
                               "user_agent"]


def get_args():
    unchecked_args = utils.parse_args([])
    if 'username' in unchecked_args.config.keys():
        return utils.parse_args(REQUIRED_CONFIG_KEYS_HOSTED)

    return utils.parse_args(REQUIRED_CONFIG_KEYS_CLOUD)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    refs = schema.pop("definitions", {})
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def get_schemas(client):
    """Build the schemas and field metadata for every stream applicable to
    this Jira instance. Cloud-only streams (e.g. `groups`, backed by
    /rest/api/2/group/bulk) have no on-prem equivalent endpoint, so they are
    left out entirely for on-prem instances."""
    schemas = {}
    field_metadata = {}
    for stream in streams_.ALL_STREAMS:
        if stream.cloud_only and client.is_on_prem_instance:
            continue

        schema = Schema.from_dict(load_schema(stream.tap_stream_id))
        schemas[stream.tap_stream_id] = schema
        field_metadata[stream.tap_stream_id] = generate_metadata(stream, schema)
    return schemas, field_metadata


def _apply_access_checks(client, schemas, field_metadata):
    """
    Probe stream access and remove inaccessible streams and their children
    from schemas and field_metadata in place.

    Most child streams rely on their parent stream's access check.
    `group_users` is an exception because it independently validates
    access to the /group/member endpoint.

    Raises JiraForbiddenError if no streams are accessible.
    """
    inaccessible_streams = [
        stream.tap_stream_id
        for stream in streams_.ALL_STREAMS
        if stream.tap_stream_id in schemas
        and not stream.check_access(client)
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise JiraForbiddenError(
            "No streams are accessible. Ensure the credentials have read permission for at least one stream."
        )
    if inaccessible_streams:
        LOGGER.warning(
            "Unauthorized streams have been excluded: %s",
            ", ".join(inaccessible_streams),
        )


def _prune_inaccessible_children(schemas, field_metadata):
    """
    Remove child streams from the catalog whose parent stream was excluded.
    Handles multi-level hierarchies. Mutates schemas and field_metadata in place.
    """
    removed = True
    while removed:
        removed = False
        for stream in streams_.ALL_STREAMS:
            parent = stream.parent_tap_stream_id
            if stream.tap_stream_id in schemas and parent and parent not in schemas:
                LOGGER.warning(
                    "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                    stream.tap_stream_id, parent,
                )
                schemas.pop(stream.tap_stream_id, None)
                field_metadata.pop(stream.tap_stream_id, None)
                removed = True


def discover(client):
    """
    Discover schemas, build metadata for all the streams and return catalog
    First, check access to all parent streams and get accessible streams
    """
    schemas, field_metadata = get_schemas(client)
    _apply_access_checks(client, schemas, field_metadata)

    catalog = Catalog([])
    for stream in streams_.ALL_STREAMS:
        if stream.tap_stream_id not in schemas:
            continue

        catalog.streams.append(CatalogEntry(
            stream=stream.tap_stream_id,
            tap_stream_id=stream.tap_stream_id,
            key_properties=stream.pk_fields,
            schema=schemas[stream.tap_stream_id],
            metadata=field_metadata[stream.tap_stream_id]))
    return catalog


def generate_metadata(stream, schema):
    mdata = metadata.new()

    # Update pk for users stream to key for on prem jira instance
    if stream.tap_stream_id == "users" and Context.client.is_on_prem_instance:
        stream.pk_fields = ["key"]

    mdata = metadata.write(mdata, (), 'table-key-properties', stream.pk_fields)
    mdata = metadata.write(mdata, (), 'forced-replication-method', stream.forced_replication_method)
    if stream.parent_tap_stream_id is not None:
        mdata = metadata.write(mdata, (), 'parent-tap-stream-id', stream.parent_tap_stream_id)

    for field_name in schema.properties.keys():
        if field_name in stream.pk_fields:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)


def output_schema(stream):
    schema = load_schema(stream.tap_stream_id)
    singer.write_schema(stream.tap_stream_id, schema, stream.pk_fields)


def sync():
    streams_.validate_dependencies()

    # two loops through streams are necessary so that the schema is output
    # BEFORE syncing any streams. Otherwise, the first stream might generate
    # data for the second stream, but the second stream hasn't output its
    # schema yet
    for stream in streams_.ALL_STREAMS:
        output_schema(stream)

    for stream in streams_.ALL_STREAMS:
        if not Context.is_selected(stream.tap_stream_id):
            continue

        # indirect_stream indicates the data for the stream comes from some
        # other stream, so we don't sync it directly.
        if stream.indirect_stream:
            continue
        Context.state["currently_syncing"] = stream.tap_stream_id
        singer.write_state(Context.state)
        try:
            stream.sync()
        except JiraForbiddenError as exc:
            # Only an HTTP 403 for this stream is tolerated; every other error
            # still fails the run. Remaining streams continue to sync.
            LOGGER.warning(
                "Unauthorized Stream: %s, skipping its sync. HTTP-Error-Message:'%s'",
                stream.tap_stream_id, exc)
    Context.state["currently_syncing"] = None
    singer.write_state(Context.state)


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = get_args()

    jira_config = args.config
    # jira client instance
    jira_client = Client(jira_config)

    # Setup Context
    Context.client = jira_client
    Context.config = jira_config
    Context.state = args.state

    try:
        if args.discover:
            discover(jira_client).dump()
            print()
        else:
            catalog = Catalog.from_dict(args.properties) \
                if args.properties else discover(jira_client)
            Context.catalog = catalog
            sync()
    finally:
        if Context.client and Context.client.login_timer:
            Context.client.login_timer.cancel()


if __name__ == "__main__":
    main()
