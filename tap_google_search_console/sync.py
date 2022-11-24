from typing import Dict

import singer
from singer import Catalog, metadata

from .client import GoogleClient as Client
from .streams import STREAMS

LOGGER = singer.get_logger()


def sync(client: Client, config: Dict, state: Dict, catalog: Catalog):
    """Sync data from tap source"""

    for stream in catalog.get_selected_streams(state):
        tap_stream_id = stream.tap_stream_id
        stream_obj = STREAMS[tap_stream_id](client, config)
        stream_schema = stream.schema.to_dict()
        stream_metadata = metadata.to_map(stream.metadata)

        LOGGER.info("Starting sync for stream: %s", tap_stream_id)

        state = singer.set_currently_syncing(state, tap_stream_id)
        singer.write_state(state)

        singer.write_schema(tap_stream_id, stream_schema, stream_obj.key_properties, stream.replication_key)

        stream_obj.sync(state, stream_schema, stream_metadata)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
