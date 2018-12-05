#!/usr/bin/env python3
import os
import json
import re
import singer
import requests
from singer import utils
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams as streams_
from .context import Context
from .http import Client

LOGGER = singer.get_logger()
REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]


STRING_TYPES = set([
    'string'
])

DATE_TYPES = set([
    'datetime',
    'date'
])

NUMBER_TYPES = set([
    'number'
])


UNKNOWN_TYPES = set([
    "any",
    "array",
    "comments-page",
    "issuetype",
    "option",
    "priority",
    "progress",
    "project",
    "resolution",
    "sd-customerrequesttype",
    "securitylevel",
    "status",
    "timetracking",
    "user",
    "votes",
    "watches"
])

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    refs = schema.pop("definitions", {})
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def sanitize(name):
    name = re.sub(r'[\s\-\/]', '_', name.lower())
    return re.sub(r'[^a-z0-9_]', '', name)


def generate_type(jira_schema):
    property_schema = {}

    field_type = jira_schema.get('type')

    if field_type in STRING_TYPES:
        property_schema['type'] = ["null","string"]
    elif field_type in DATE_TYPES:
        date_type = {"type": "string", "format": "date-time"}
        string_type = {"type": ["string", "null"]}
        property_schema["anyOf"] = [date_type, string_type]
    elif field_type in NUMBER_TYPES:
        property_schema['type'] = ["null","number"]
    else:
        property_schema['type'] = {}

    # { "type": [ "null", inferred_type] <"format": "date-time">}
    return property_schema

def discover(config):
    catalog = Catalog([])
    for stream in streams_.all_streams:
        schema_data = load_schema(stream.tap_stream_id)

        # The Issues stream gets special treatment
        if stream.tap_stream_id == "issues":
            # Make a request to fields endpoint
            resp = Context.client.request("fields", "GET", "/rest/api/3/field")

            issue_resp = Context.client.request("issues", "GET", "/rest/api/3/issue/GEN122DCEA-1")

            fields_schemas = []
            fields_names = []
            fields_ids = []
            droppable_fields_fields = set()
            problem_resp_items = []
            for thing in resp:
                # Get fields of interest

                # if thing['key'] == "":
                import ipdb; ipdb.set_trace()
                1+1
                
                try:
                    fields_schemas.append(thing['schema'])
                except KeyError:
                    fields_schemas.append('NO_SCHEMA_FOUND')
                    problem_resp_items.append(thing)
                    continue
                fields_names.append(thing.get('name','NO_NAME_FOUND'))
                fields_ids.append(thing.get('id','NO_ID_FOUND'))

                # Save all other fields
                for trash in thing.keys():
                    if trash not in ['id','name','schema']:
                        droppable_fields_fields.add(trash)

            issues_keys = list(issue_resp.keys())
            issues_fields_keys = list(issue_resp['fields'].keys())
                        
                # if thing['id'] == 'customfield_10076':

                #     bubble_up = {}
                #     for k,v in issue_resp.items():
                #         if k == 'fields':
                #             for k2,v2 in issue_resp['fields'].items():

                #                 if k2 == 'customfield_10076':
                #                     bubble_up[thing['name']+'custom'] = v2
                #                 else:
                #                     bubble_up[k2] = v2
                #         else:
                #             bubble_up[k] = v
                    
                #     import ipdb; ipdb.set_trace()
                #     1+1
                
            import ipdb; ipdb.set_trace()
            1+1
            
            # iterate on that response; generate a schema
            for obj in resp:
                if not obj['key'].startswith("customfield"):
                    schema_data['properties'][obj['key']] = {}
                    continue

                sanitized_name = sanitize(obj['name'])
                # Check to make sure the sanitized name isn't in the properties already; if it is, it needs more sanitizing
                #if sanitized_name in schema_data['properties']

                # If the object has a schema key, we can infer it
                if obj.get('schema'):
                    schema_data['properties'][sanitized_name] = generate_type(obj['schema'])
                else:
                    # MAybe add a metadata-unsupported thing like Salesforce?
                    pass

        # if stream.tap_stream_id == "issues":
        #     import ipdb; ipdb.set_trace()
        #     1+1
        schema = Schema.from_dict(schema_data)

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
    #mdata = metadata.write(mdata, (), 'forced-replication-method', stream.replication_method)

    #if stream.replication_key:
    #    mdata = metadata.write(mdata, (), 'valid-replication-keys', [stream.replication_key])

    for field_name in schema.properties.keys():
        if field_name in stream.pk_fields: #or field_name == stream.replication_key:
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
        Context.write_state()
        stream.sync()
    Context.state["currently_syncing"] = None
    Context.write_state()


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # Setup Context
    Context.config = args.config
    Context.state = args.state

    try:
        Context.client = Client(Context.config)

        Context.client.refresh_credentials()
        Context.client.test_credentials_are_authorized()

        if args.discover:
            discover(args.config).dump()
            print()
        elif args.properties:
            Context.catalog = Catalog.from_dict(args.properties)
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
