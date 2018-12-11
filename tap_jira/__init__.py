#!/usr/bin/env python3
import os
import json
import singer
from singer import utils
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams as streams_
from .context import Context
from .http import Client

LOGGER = singer.get_logger()
REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    refs = schema.pop("definitions", {})
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def discover(config):
    catalog = Catalog([])
    for stream in streams_.all_streams:
        schema = Schema.from_dict(load_schema(stream.tap_stream_id))

        mdata = generate_metadata(stream, schema)

        catalog.streams.append(CatalogEntry(
            stream=stream.tap_stream_id,
            tap_stream_id=stream.tap_stream_id,
            key_properties=stream.pk_fields,
            schema=schema,
            metadata=mdata))
    return catalog


def generate_metadata(stream, schema):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', stream.pk_fields)

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
    currently_syncing = Context.state.get("currently_syncing")
    start_idx = streams_.all_stream_ids.index(currently_syncing) \
        if currently_syncing else 0

    # two loops through streams are necessary so that the schema is output
    # BEFORE syncing any streams. Otherwise, the first stream might generate
    # data for the second stream, but the second stream hasn't output its
    # schema yet
    for stream in streams_.all_streams:
        output_schema(stream)

    for stream in streams_.all_streams:
        if not Context.is_selected(stream.tap_stream_id):
            continue

        # indirect_stream indicates the data for the stream comes from some
        # other stream, so we don't sync it directly.
        if stream.indirect_stream:
            continue
        Context.state["currently_syncing"] = stream.tap_stream_id
        singer.write_state(Context.state)
        stream.sync()
    Context.state["currently_syncing"] = None
    singer.write_state(Context.state)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # Setup Context
    catalog = Catalog.from_dict(args.properties) \
        if args.properties else discover(args.config)
    Context.config = args.config
    Context.state = args.state
    Context.catalog = catalog

    try:
        Context.client = Client(Context.config)

        Context.client.refresh_credentials()
        Context.client.test_credentials_are_authorized()

        if args.discover:
            discover(args.config).dump()
            print()
        else:
            sync()
    finally:
        if Context.client and Context.client.login_timer:
            Context.client.login_timer.cancel()


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == "__main__":
    main()
