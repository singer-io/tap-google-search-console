from .abstract import FullTableStream
from singer.logger import get_logger

LOGGER = get_logger()

class Sites(FullTableStream):
    """
    Class Representing the `Sites` Stream
    """
    tap_stream_id = "sites"
    key_properties = ['site_url']

    data_key= 'site_entry'


    def __init__(self,client=None) -> None:
        LOGGER.info("invoked %s",self.__class__)
        self.client = client

    def get_records(self):
        LOGGER.info("get records called from %s",self.__class__)

    def sync(self):
        LOGGER.info("sync called from %s",self.__class__)

