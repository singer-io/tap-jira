#!/usr/bin/env python3
import os
import json
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams as streams_
from .context import Context
from .http import Client

REQUIRED_CONFIG_KEYS = ["start_date", "username", "password", "base_url"]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    refs = schema.pop("definitions", {})
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def test_credentials_are_authorized(config):
    client = Client(config)
    client.request(streams_.ISSUES.tap_stream_id, "GET", "/rest/api/2/search",
                   params={"maxResults": 1})


def discover(config):
    test_credentials_are_authorized(config)
    catalog = Catalog([])
    for stream in streams_.all_streams:
        schema = Schema.from_dict(load_schema(stream.tap_stream_id),
                                  inclusion="automatic")
        catalog.streams.append(CatalogEntry(
            stream=stream.tap_stream_id,
            tap_stream_id=stream.tap_stream_id,
            key_properties=stream.pk_fields,
            schema=schema,
        ))
    return catalog


def output_schema(stream):
    schema = load_schema(stream.tap_stream_id)
    singer.write_schema(stream.tap_stream_id, schema, stream.pk_fields)


def sync(ctx):
    currently_syncing = ctx.state.get("currently_syncing")
    start_idx = streams_.all_stream_ids.index(currently_syncing) \
        if currently_syncing else 0
    stream_ids_to_sync = [cs.tap_stream_id for cs in ctx.catalog.streams
                          if cs.is_selected()]
    streams = [s for s in streams_.all_streams[start_idx:]
               if s.tap_stream_id in stream_ids_to_sync]
    # two loops through streams are necessary so that write_to_stdout is set
    # for all appropriate streams BEFORE syncing any streams. Otherwise, the
    # first stream might generate data for the second stream, but the second
    # stream hasn't set write_to_stdout yet
    for stream in streams:
        output_schema(stream)
        stream.write_to_stdout = True
    for stream in streams:
        # indirect_stream indicates the data for the stream comes from some
        # other stream, so we don't sync it directly.
        if stream.indirect_stream:
            continue
        ctx.state["currently_syncing"] = stream.tap_stream_id
        ctx.write_state()
        stream.sync(ctx)
    ctx.state["currently_syncing"] = None
    ctx.write_state()


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        discover(args.config).dump()
        print()
    else:
        if not args.properties:
            raise Exception("--properties is a required argument when syncing.")
        catalog = Catalog.from_dict(args.properties)
        sync(Context(args.config, args.state, catalog))

if __name__ == "__main__":
    main()
