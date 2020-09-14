import singer
from singer import Transformer, metadata

from .client import Client
from .streams import STREAMS
from .utils import validate_dependencies, is_selected, deep_get, split_stream

LOGGER = singer.get_logger()

schemas_written = []


def _sync_streams(client, streams, transformer,
                  config, state, catalog, **kwargs):
    for s in streams:
        stream, substreams = split_stream(s)
        if not is_selected(stream, catalog):
            LOGGER.info(f'{stream.tap_stream_id} not selected')
        else:
            kwargs['substreams'] = substreams
            _sync_stream(client, stream, transformer,
                         config, state, catalog, **kwargs)


def _sync_stream(client, stream, transformer,
                 config, state, catalog, **kwargs):
    record = kwargs.get('record', None)
    substreams = kwargs.get('substreams')
    tap_stream_id = stream.tap_stream_id

    stream_obj = stream()
    stream_catalog = catalog.get_stream(stream.tap_stream_id)
    replication_key = stream_obj.replication_key
    stream_schema = stream_catalog.schema.to_dict()
    stream_metadata = metadata.to_map(stream_catalog.metadata)
    replication_method = metadata.get(
        stream_metadata, (), 'replication-method')
    stream_obj.update_replication_method(replication_method)

    LOGGER.debug('Starting sync for stream: %s', tap_stream_id)
    state = singer.set_currently_syncing(state, tap_stream_id)
    singer.write_state(state)

    # Only write schema once
    if not tap_stream_id in schemas_written:
        singer.write_schema(
            tap_stream_id,
            stream_schema,
            stream_obj.key_properties,
            stream.replication_key
        )
        schemas_written.append(tap_stream_id)

    start_date = singer.get_bookmark(
        state, tap_stream_id, replication_key, config['start_date'])
    offset = singer.get_bookmark(state, tap_stream_id, 'offset', 0)

    max_record_value = start_date
    for page, cursor in stream_obj.sync(
            client,
            config,
            state,
            record=record,
            start_date=start_date,
            offset=offset
    ):
        for record in page:
            transformed_record = transformer.transform(
                record, stream_schema, stream_metadata)

            time_extracted = singer.utils.now()
            singer.write_record(
                tap_stream_id,
                transformed_record,
                time_extracted=time_extracted
            )

            if stream_obj.replication_method == 'INCREMENTAL':
                current_replication_value = deep_get(
                    record, replication_key)
                if current_replication_value \
                        and current_replication_value > max_record_value:
                    max_record_value = current_replication_value

            if substreams:
                _sync_streams(client, substreams.values(),
                              transformer, config, state, catalog,
                              record=record, start_date=start_date)

        state = singer.write_bookmark(
            state, tap_stream_id, 'offset', cursor)

        if stream_obj.replication_method == 'INCREMENTAL':
            state = singer.write_bookmark(
                state, tap_stream_id, replication_key, max_record_value)

        singer.write_state(state)

    state = singer.clear_bookmark(state, tap_stream_id, 'offset')
    singer.write_state(state)


def sync(config, state, catalog):
    validate_dependencies(STREAMS, catalog)

    client = Client(config)
    with Transformer() as transformer:
        streams = STREAMS.values()
        _sync_streams(client, streams, transformer, config, state, catalog)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
