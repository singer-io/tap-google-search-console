#!/usr/bin/env python3
from singer import get_logger, utils

from tap_google_search_console.client import GoogleClient
from tap_google_search_console.discover import discover
from tap_google_search_console.sync import sync

LOGGER = get_logger()

REQUIRED_CONFIG_KEYS = ["client_id", "client_secret", "refresh_token", "start_date", "user_agent"]


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    parsed_args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    with GoogleClient(
        parsed_args.config["client_id"],
        parsed_args.config["client_secret"],
        parsed_args.config["refresh_token"],
        parsed_args.config["site_urls"],
        parsed_args.config["user_agent"],
        parsed_args.config.get("request_timeout"),
    ) as client:
        if parsed_args.discover:
            catalog = discover(client)
            catalog.dump()
        else:
            catalog = parsed_args.catalog or discover(client)
            sync(client, parsed_args.config, parsed_args.state, catalog)


if __name__ == "__main__":
    main()
