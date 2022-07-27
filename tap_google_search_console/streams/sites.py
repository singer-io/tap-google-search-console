from .abstract import FullTableStream
from singer.logger import get_logger

LOGGER = get_logger()


class Sites(FullTableStream):
    """
    Class Representing the `Sites` Stream
    """

    tap_stream_id = "sites"
    key_properties = ["site_url",]

    data_key = "site_entry"
    path = ("sites/{}",)

    def __init__(self, client=None) -> None:
        # LOGGER.info("invoked %s", self.__class__) ss
        super().__init__(client)

    def get_records(self):
        pass
        # LOGGER.info("get records called from %s", self.__class__)
