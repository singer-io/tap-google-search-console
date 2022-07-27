from .abstract import FullTableStream
from singer.logger import get_logger

LOGGER = get_logger()


class Sitemaps(FullTableStream):
    """
    Class Representing the `Sitemaps` Stream
    """

    tap_stream_id = "sitemaps"
    key_properties = ("site_url", "path", "last_submitted")

    data_key = "sitemap"
    path = ("sites/{}/sitemaps",)

    def __init__(self, client=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client)

    def get_records(self):
        LOGGER.info("get records called from %s", self.__class__)

    def sync(self):
        LOGGER.info("sync called from %s", self.__class__)
