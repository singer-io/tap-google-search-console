from typing import Dict, Iterator

from singer.logger import get_logger

from tap_google_search_console.helpers import encode_and_format_url, transform_json

from .abstract import FullTableStream

LOGGER = get_logger()


class Sitemaps(FullTableStream):
    """Class Representing the `Sitemaps` Stream."""

    tap_stream_id = "sitemaps"
    key_properties = ["site_url", "path", "last_submitted"]

    data_key = "sitemap"
    path = "sites/{}/sitemaps"

    def get_records(self) -> Iterator[Dict]:
        """Performs API calls to extract data for each site."""
        LOGGER.info("get records called from %s", self.__class__)
        transformed_data = []
        for site in self.get_site_url():
            if site[:9] == "sc-domain":
                LOGGER.info(f"Skipping Site: {site}")
                LOGGER.info("Sitemaps API does not support domain property urls at this time.")
                continue
            path = encode_and_format_url(site, self.path)
            data = self.client.get(path)
            if not data:
                return transformed_data
            transformed_data.extend(
                iter(transform_json(data, self.tap_stream_id, site=site, path=self.data_key).get(self.data_key, []))
            )
        yield from transformed_data
