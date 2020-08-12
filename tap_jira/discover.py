import os
import json
import singer
from .streams import STREAMS
from .utils import flatten_streams
from singer.catalog import Catalog
from singer import metadata

LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas():
    schemas = {}
    schemas_metadata = {}

    streams = flatten_streams(STREAMS, {})
    for stream_name, stream_object in streams.items():
        LOGGER.info('Getting schema for {}'.format(stream_name))
        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path) as file:
            schema = json.load(file)

        refs = schema.pop("definitions", {})
        if refs:
            singer.resolve_schema_references(schema, refs)

        meta = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_object.key_properties,
            replication_method=stream_object.replication_method
        )

        meta = metadata.to_map(meta)

        if stream_object.replication_key:
            meta = metadata.write(
                meta, ('properties', stream_object.replication_key), 'inclusion', 'automatic')

        meta = metadata.to_list(meta)

        schemas[stream_name] = schema
        schemas_metadata[stream_name] = meta

    return schemas, schemas_metadata


def discover():
    schemas, schemas_metadata = get_schemas()
    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = schemas_metadata[schema_name]

        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': schema_meta
        }

        streams.append(catalog_entry)

    return Catalog.from_dict({'streams': streams})
