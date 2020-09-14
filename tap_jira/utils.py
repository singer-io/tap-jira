import os
import singer
from singer import utils
from functools import reduce

LOGGER = singer.get_logger()


class DependencyException(Exception):
    def __init__(self, errors):
        self.errors = errors

    def __str__(self):
        return " ".join(self.errors)


def is_selected(stream, catalog):
    stream_cls = stream()
    catalog_entry = catalog.get_stream(stream_cls.tap_stream_id)
    return True if (catalog_entry and catalog_entry.is_selected()) else False


def split_stream(stream):
    substreams = {}
    if isinstance(stream, dict):
        if 'substreams' in stream.keys():
            substreams = stream['substreams']
        stream = stream['cls']

    return stream, substreams


def deep_get(dictionary, keys, default=None):
    return reduce(lambda d, key: d.get(key, default)
                  if isinstance(d, dict)
                  else default, keys.split("."),
                  dictionary)


def check_substream(stream, catalog, errs=[]):
    if not isinstance(stream, dict):
        return errs

    msg_tmpl = ("Unable to extract {0} data. "
                "To receive {0} data, you also need to select {1}.")

    subs = stream['substreams']
    selected = is_selected(stream['cls'], catalog)
    for substream in subs.values():
        if isinstance(substream, dict):
            check_substream(substream, catalog, errs=errs)
        else:
            sub_selected = is_selected(substream, catalog)
            if not selected and sub_selected:
                errs.append(msg_tmpl.format(
                    substream.tap_stream_id, stream['cls'].tap_stream_id))

    return errs


def flatten_streams(streams, flat_streams={}):
    for name, stream_object in streams.items():
        substreams = {}
        if isinstance(stream_object, dict):
            substreams = stream_object.get('substreams', {})
            stream_object = stream_object['cls']

        flat_streams[name] = stream_object

        if substreams:
            flatten_streams(substreams, flat_streams)

    return flat_streams


def validate_dependencies(streams, catalog):
    errs = []
    for value in streams.values():
        errs.extend(check_substream(value, catalog, []))

    if errs:
        LOGGER.info(f"Found {len(errs)} validation error(s)")
        raise DependencyException(errs)


def raise_if_bookmark_cannot_advance(worklogs):
    # Worklogs can only be queried with a `since` timestamp and
    # provides no way to page through the results. The `since`
    # timestamp has <=, not <, semantics. It also caps the response at
    # 1000 objects. Because of this, if we ever see a page of 1000
    # worklogs that all have the same `updated` timestamp, we cannot
    # tell whether we in fact got all the updates and so we need to
    # raise.
    #
    # That said, a page of 999 worklogs that all have the same
    # timestamp is fine. That just means that 999 worklogs were
    # updated at the same timestamp but that we did, in fact, get them
    # all.
    #
    # The behavior, then, always resyncs the latest `updated`
    # timestamp, no matter how many results are there. If you have 500
    # worklogs updated at T1 and 999 worklogs updated at T2 and
    # `last_updated` is set to T1, the first trip through this will
    # see 1000 items, 500 of which have `updated==T1` and 500 of which
    # have `updated==T2`. Then, `last_updated` is set to T2 and due to
    # the <= semantics, you grab the 999 T2 worklogs which passes this
    # function because there's less than 1000 worklogs of
    # `updated==T2`.
    #
    # OTOH, if you have 1 worklog with `updated==T1` and 1000 worklogs
    # with `updated==T2`, first trip you see 1 worklog at T1 and 999
    # at T2 which this code will think is fine, but second trip
    # through you'll see 1000 worklogs at T2 which will fail
    # validation (because we can't tell whether there would be more
    # that should've been returned).
    LOGGER.debug('Worklog page count: `%s`', len(worklogs))
    worklog_updates = [utils.strptime_to_utc(w['updated'])
                       for w in worklogs]
    min_updated = min(worklog_updates)
    max_updated = max(worklog_updates)
    LOGGER.debug('Worklog min updated: `%s`', min_updated)
    LOGGER.debug('Worklog max updated: `%s`', max_updated)
    if len(worklogs) == 1000 and min_updated == max_updated:
        raise Exception(("Worklogs bookmark can't safely advance."
                         "Every `updated` field is `{}`")
                        .format(worklog_updates[0]))


def advance_bookmark(worklogs):
    raise_if_bookmark_cannot_advance(worklogs)
    new_last_updated = max(utils.strptime_to_utc(w["updated"])
                           for w in worklogs)
    return new_last_updated


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    refs = schema.pop("definitions", {})
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def write_schema(stream):
    schema = load_schema(stream.tap_stream_id)
    singer.write_schema(stream.tap_stream_id, schema, stream.pk_fields)
