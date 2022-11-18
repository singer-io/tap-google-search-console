from .abstract import FullTableStream
from tap_google_search_console.helpers import encode_and_format_url, transform_json
from singer.logger import get_logger
from typing import Iterator, Dict

LOGGER = get_logger()


class Sites(FullTableStream):
    """
    Class Representing the `Sites` Stream
    """

    tap_stream_id = "sites"
    key_properties = ["site_url", ]

    data_key = "site_entry"
    path = "sites/{}"

    def __init__(self, client=None, config=None) -> None:
        # LOGGER.info("invoked %s", self.__class__) ss
        super().__init__(client, config)

    def get_records(self) -> Iterator[Dict]:
        """
        Performs API calls to extract data for each site
        """
        records = []
        for site in self.get_site_url():
            path = encode_and_format_url(site, self.path)
            data = self.client.get(path)
            records.append(data)
        data = {self.data_key: records}
        # transforms data by converting camelCase fields to snake_case fields
        transformed_records = transform_json(data, self.tap_stream_id)
        yield from transformed_records.get(self.data_key, [])
