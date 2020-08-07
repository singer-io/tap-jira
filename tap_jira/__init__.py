#!/usr/bin/env python3
import json
import singer
from singer.catalog import write_catalog
from .discover import discover
from .sync import sync

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
    unchecked_args = singer.utils.parse_args([])
    if 'username' in unchecked_args.config.keys():
        return singer.utils.parse_args(REQUIRED_CONFIG_KEYS_HOSTED)

    return singer.utils.parse_args(REQUIRED_CONFIG_KEYS_CLOUD)


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = get_args()

    catalog = args.catalog if args.catalog else discover()

    if args.discover:
        write_catalog(catalog)
    else:
        sync(args.config, args.state, catalog)


if __name__ == '__main__':
    main()
