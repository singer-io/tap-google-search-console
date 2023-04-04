import json
from typing import Dict, Tuple

from singer.catalog import Catalog

from tap_google_search_console.client import GoogleClient
from tap_google_search_console.helpers import get_abs_path
from tap_google_search_console.streams import STREAMS


def get_schemas() -> Tuple[Dict, Dict]:
    """Builds the singer schema and metadata dictionaries."""
    streams, stream_metadata = {}, {}

    for stream_name, stream in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")

        with open(schema_path, encoding="utf-8") as file:
            schema = json.load(file)

        streams[stream_name], stream_metadata[stream_name] = schema, stream.get_metadata(schema)

    return streams, stream_metadata


def discover(client: GoogleClient):
    """Performs permission check and starts discover."""
    client.check_sites_access()
    schemas, schema_metadata = get_schemas()
    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = schema_metadata[schema_name]
        streams.append({"stream": schema_name, "tap_stream_id": schema_name, "schema": schema, "metadata": schema_meta})
    return Catalog.from_dict({"streams": streams})
