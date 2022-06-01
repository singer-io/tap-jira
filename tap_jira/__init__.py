#!/usr/bin/env python3
import os
import json
import singer
from copy import deepcopy
from singer import utils
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams as streams_
from .context import Context
from .http import Client

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


def discover():
    catalog = Catalog([])
    for stream in streams_.ALL_STREAMS:
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

    # Update pk for users stream to key for on prem jira instance
    if stream.tap_stream_id == "users" and Context.client.is_on_prem_instance:
        stream.pk_fields = ["key"]

    mdata = metadata.write(mdata, (), 'table-key-properties', stream.pk_fields)

    for field_name in schema.properties.keys():
        if field_name in stream.pk_fields:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)

def is_object_type(property_schema):
    """Return true if the JSON Schema type is an object or None if detection fails.
    This code is based on https://github.com/meltano/sdk/blob/c9c0967b0caca51fe7c87082f9e7c5dd54fa5dfa/singer_sdk/helpers/_typing.py#L50
    """
    if "anyOf" not in property_schema and "type" not in property_schema:
        return None  # Could not detect data type
    for property_type in property_schema.get("anyOf", [property_schema.get("type")]):
        if "object" in property_type or property_type == "object":
            return True
    return False


def is_property_selected(
    stream_name,
    breadcrumb,
):
    """Return True if the property is selected for extract.
    Breadcrumb of `[]` or `None` indicates the stream itself. Otherwise, the
    breadcrumb is the path to a property within the stream.
    The code is based on https://github.com/meltano/sdk/blob/c9c0967b0caca51fe7c87082f9e7c5dd54fa5dfa/singer_sdk/helpers/_catalog.py#L63
    """
    breadcrumb = breadcrumb or tuple()
    if isinstance(breadcrumb, str):
        breadcrumb = tuple([breadcrumb])

    if not Context.catalog:
        return True

    catalog_entry = Context.get_catalog_entry(stream_name).to_dict()
    if not catalog_entry:
        LOGGER.warning(f"Catalog entry missing for '{stream_name}'. Skipping.")
        return False

    if not catalog_entry.get('metadata'):
        return True

    md_map = metadata.to_map(catalog_entry['metadata'])
    md_entry = md_map.get(breadcrumb)
    parent_value = None
    if len(breadcrumb) > 0:
        parent_breadcrumb = tuple(list(breadcrumb)[:-2])
        parent_value = is_property_selected(
            stream_name, parent_breadcrumb
        )
    if parent_value is False:
        return parent_value

    if not md_entry:
        LOGGER.warning(
            f"Catalog entry missing for '{stream_name}':'{breadcrumb}'. "
            f"Using parent value of selected={parent_value}."
        )
        return parent_value or False

    if md_entry.get("inclusion") == "unsupported":
        return False

    if md_entry.get("inclusion") == "automatic":
        if md_entry.get("selected") is False:
            LOGGER.warning(
                f"Property '{':'.join(breadcrumb)}' was deselected while also set"
                "for automatic inclusion. Ignoring selected==False input."
            )
        return True

    if "selected" in md_entry:
        return bool(md_entry['selected'])

    if md_entry.get('inclusion') == 'available':
        return True

    raise ValueError(
        f"Could not detect selection status for '{stream_name}' breadcrumb "
        f"'{breadcrumb}' using metadata: {md_map}"
    )


def pop_deselected_schema(
    schema,
    stream_name,
    breadcrumb,
):
    """Remove anything from schema that is not selected.
    Walk through schema, starting at the index in breadcrumb, recursively updating in
    place.
    This code is based on https://github.com/meltano/sdk/blob/c9c0967b0caca51fe7c87082f9e7c5dd54fa5dfa/singer_sdk/helpers/_catalog.py#L146
    """
    for property_name, val in list(schema.get("properties", {}).items()):
        property_breadcrumb = tuple(
            list(breadcrumb) + ["properties", property_name]
        )
        selected = is_property_selected(
            stream_name, property_breadcrumb
        )
        if not selected:
            schema["properties"].pop(property_name)
            continue

        if is_object_type(val):
            # call recursively in case any subproperties are deselected.
            pop_deselected_schema(
                val, stream_name, property_breadcrumb
            )


def output_schema(stream):
    stream_id = stream.tap_stream_id
    catalog_entry = Context.get_catalog_entry(stream_id).to_dict()
    if Context.is_selected(stream_id):
        schema = deepcopy(catalog_entry['schema'])
        pop_deselected_schema(schema, stream_id, tuple())
        singer.write_schema(stream_id, schema, stream.pk_fields)


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
        stream.sync()
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
    catalog = Catalog.from_dict(args.properties) \
        if args.properties else discover()
    Context.config = jira_config
    Context.state = args.state
    Context.catalog = catalog

    try:
        if args.discover:
            discover().dump()
            print()
        else:
            sync()
    finally:
        if Context.client and Context.client.login_timer:
            Context.client.login_timer.cancel()


if __name__ == "__main__":
    main()
