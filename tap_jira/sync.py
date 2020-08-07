import singer
from singer import Transformer, metadata

from .client import Client
from .streams import STREAMS
from .utils import validate_dependencies, is_selected

LOGGER = singer.get_logger()


def _sync_stream(client, stream, transformer,
                 config, state, catalog, **kwargs):
    substreams = {}
    if isinstance(stream, dict):
        if 'substreams' in stream.keys():
            substreams = stream['substreams']
        stream = stream['cls']

    if not is_selected(stream, catalog):
        LOGGER.info(f'{stream.tap_stream_id} not selected')
        return

    tap_stream_id = stream.tap_stream_id

    stream_obj = stream()
    stream_catalog = catalog.get_stream(stream.tap_stream_id)
    replication_key = stream_obj.replication_key
    stream_schema = stream_catalog.schema.to_dict()
    stream_metadata = metadata.to_map(stream_catalog.metadata)

    LOGGER.info('Starting sync for stream: %s', tap_stream_id)
    state = singer.set_currently_syncing(state, tap_stream_id)
    singer.write_state(state)

    singer.write_schema(
        tap_stream_id,
        stream_schema,
        stream_obj.key_properties,
        stream.replication_key
    )

    start_date = singer.get_bookmark(
        state, tap_stream_id, replication_key, config['start_date'])
    page_num = singer.get_bookmark(
        state, tap_stream_id, 'page_num', 0)
    record = kwargs.get('record', None)

    if stream_obj.replication_method == 'INCREMENTAL':
        for page, cursor in stream_obj.sync(
                client,
                config,
                state,
                record=record,
                start_date=start_date,
                page_num=page_num
        ):
            for record in page:
                transformed_record = transformer.transform(
                    record, stream_schema, stream_metadata)
                singer.write_record(
                    tap_stream_id,
                    transformed_record)

                if record[replication_key] > max_record_value:
                    max_record_value = transformed_record[replication_key]

                if substreams:
                    for substream in substreams.values():
                        _sync_stream(client, substream, transformer, config, state, catalog,
                                     record=record,
                                     start_date=start_date,
                                     page_num=page_num)

            state = singer.write_bookmark(
                state, tap_stream_id, 'page_num', cursor)
            state = singer.write_bookmark(
                state, tap_stream_id, replication_key, max_record_value)
            singer.write_state(state)

    else:
        for page, cursor in stream_obj.sync(
                client,
                config,
                state,
                record=record,
                start_date=start_date,
                page_num=page_num
        ):
            for record in page:
                singer.write_record(
                    tap_stream_id,
                    transformer.transform(
                        record, stream_schema, stream_metadata,
                    ))
                if substreams:
                    for substream in substreams.values():
                        _sync_stream(client, substream, transformer, config, state, catalog,
                                     record=record,
                                     start_date=start_date,
                                     page_num=page_num)

            state = singer.write_bookmark(
                state, tap_stream_id, 'page_num', cursor)
            singer.write_state(state)

    state = singer.clear_bookmark(state, tap_stream_id, 'page_num')
    singer.write_state(state)


def sync(config, state, catalog):
    validate_dependencies(STREAMS, catalog)

    client = Client(config)
    with Transformer() as transformer:
        for stream in STREAMS.values():
            _sync_stream(client, stream, transformer, config, state, catalog)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
