#!/usr/bin/env python3
from singer import utils,get_logger
from tap_google_search_console.discover import discover
from tap_google_search_console.sync import sync

LOGGER = get_logger()

REQUIRED_CONFIG_KEYS = [
    'client_id',
    'client_secret',
    'refresh_token',
    'start_date',
    'user_agent'
]



@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(args.config)
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover(args.config)
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
