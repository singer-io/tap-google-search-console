from singer.logger import get_logger
from .abstract import IncremetalStream

LOGGER = get_logger()


class PerformanceReportCustom(IncremetalStream):
    """
    Class Representing the `performance_report_custom` Stream
    """

    tap_stream_id = "performance_report_custom"
    key_properties = ("site_url", "search_type", "date", "dimensions_hash_key")
    replication_key = "date"
    valid_replication_keys = ("date",)

    data_key = "rows"
    path = "sites/{}/searchAnalytics/query"

    def __init__(self, client=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client)

    def get_records(self):
        LOGGER.info("get records called from %s", self.__class__)

    def sync(self):
        LOGGER.info("sync called from %s", self.__class__)


class PerformanceReportDate(IncremetalStream):
    """
    Class Representing the `performance_report_date` Stream
    """

    tap_stream_id = "performance_report_date"
    key_properties = ("site_url", "search_type", "date")
    replication_key = "date"

    valid_replication_keys = ("date",)

    data_key = "rows"
    path = "sites/{}/searchAnalytics/query"

    def __init__(self, client=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client)

    def get_records(self):
        LOGGER.info("get records called from %s", self.__class__)

    def sync(self):
        LOGGER.info("sync called from %s", self.__class__)


class PerformanceReportCountry(IncremetalStream):
    """
    Class Representing the `performance_report_country` Stream
    """

    tap_stream_id = "performance_report_country"
    key_properties = ["site_url", "search_type", "date", "country"]
    replication_key = "date"

    valid_replication_keys = ["date"]

    data_key = "rows"
    path = "sites/{}/searchAnalytics/query"

    def __init__(self, client=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client)

    def get_records(self):
        LOGGER.info("get records called from %s", self.__class__)

    def sync(self):
        LOGGER.info("sync called from %s", self.__class__)


class PerformanceReportDevices(IncremetalStream):
    """
    Class Representing the `performance_report_device` Stream
    """

    tap_stream_id = "performance_report_device"
    key_properties = ["site_url", "search_type", "date", "device"]
    replication_key = "date"

    valid_replication_keys = ["date"]

    data_key = "rows"
    path = "sites/{}/searchAnalytics/query"

    def __init__(self, client=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client)

    def get_records(self):
        LOGGER.info("get records called from %s", self.__class__)

    def sync(self):
        LOGGER.info("sync called from %s", self.__class__)


class PerformanceReportPage(IncremetalStream):
    """
    Class Representing the `performance_report_page` Stream
    """

    tap_stream_id = "performance_report_page"
    key_properties =("site_url", "search_type", "date", "page")
    replication_key = "date"

    valid_replication_keys = ("date","page")

    data_key = "rows"
    path = "sites/{}/searchAnalytics/query"

    def __init__(self, client=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client)

    def get_records(self):
        LOGGER.info("get records called from %s", self.__class__)

    def sync(self):
        LOGGER.info("sync called from %s", self.__class__)


class PerformanceReportQuery(IncremetalStream):
    """
    Class Representing the `performance_report_query` Stream
    """

    tap_stream_id = "performance_report_query"
    key_properties = ["site_url", "search_type", "date", "query"]
    replication_key = "date"

    valid_replication_keys = ["date"]

    data_key = "rows"
    path = "sites/{}/searchAnalytics/query"

    def __init__(self, client=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client)

    def get_records(self):
        LOGGER.info("get records called from %s", self.__class__)

    def sync(self):
        LOGGER.info("sync called from %s", self.__class__)
