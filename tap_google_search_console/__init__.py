#!/usr/bin/env python3

import sys
import json
import argparse
import singer
from singer import metadata, utils
from tap_google_search_console.client import GoogleClient
from tap_google_search_console.discover import discover
from tap_google_search_console.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'client_id',
    'client_secret',
    'refresh_token',
    'start_date',
    'user_agent'
]

def do_discover(client):

    LOGGER.info('Starting discover')
    client.check_sites_access()
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with GoogleClient(parsed_args.config['client_id'],
                      parsed_args.config['client_secret'],
                      parsed_args.config['refresh_token'],
                      parsed_args.config['site_urls'],
                      parsed_args.config['user_agent'],
                      parsed_args.config.get('request_timeout')) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        if parsed_args.discover:
            do_discover(client)
        elif parsed_args.catalog:
            sync(client=client,
                 config=parsed_args.config,
                 catalog=parsed_args.catalog,
                 state=state)

if __name__ == '__main__':
    main()
