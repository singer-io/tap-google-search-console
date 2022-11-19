from singer.logger import get_logger

from .abstract import IncrementalTableStream
from typing import Dict, List

LOGGER = get_logger()


class PerformanceReportCustom(IncrementalTableStream):
    """
    Class Representing the `performance_report_custom` Stream
    """

    tap_stream_id = "performance_report_custom"
    key_properties = ["site_url", "search_type", "date", "dimensions_hash_key"]
    replication_key = "date"
    valid_replication_keys = ("date",)

    body_params = {'aggregationType': 'auto'}
    dimension_list = ['date', 'country', 'device', 'page', 'query']

    def __init__(self, client=None, config=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client, config)


class PerformanceReportDate(IncrementalTableStream):
    """
    Class Representing the `performance_report_date` Stream
    """

    tap_stream_id = "performance_report_date"
    key_properties = ["site_url", "search_type", "date"]
    replication_key = "date"
    valid_replication_keys = ("date",)

    body_params = {'aggregationType': 'byProperty', 'dimensions': ['date']}

    def __init__(self, client=None, config=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client, config)


class PerformanceReportCountry(IncrementalTableStream):
    """
    Class Representing the `performance_report_country` Stream
    """

    tap_stream_id = "performance_report_country"
    key_properties = ["site_url", "search_type", "date", "country"]
    replication_key = "date"
    valid_replication_keys = ("date",)

    body_params = {'aggregationType': 'byProperty', 'dimensions': ['date', 'country']}

    def __init__(self, client=None, config=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client, config)


class PerformanceReportDevices(IncrementalTableStream):
    """
    Class Representing the `performance_report_device` Stream
    """

    tap_stream_id = "performance_report_device"
    key_properties = ["site_url", "search_type", "date", "device"]
    replication_key = "date"
    valid_replication_keys = ("date",)

    body_params = {'aggregationType': 'byProperty', 'dimensions': ['date', 'device']}

    def __init__(self, client=None, config=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client, config)


class PerformanceReportPage(IncrementalTableStream):
    """
    Class Representing the `performance_report_page` Stream
    """

    tap_stream_id = "performance_report_page"
    key_properties = ["site_url", "search_type", "date", "page"]
    replication_key = "date"
    valid_replication_keys = ("date", "page")

    body_params = {'aggregationType': 'byPage', 'dimensions': ['date', 'page']}

    def __init__(self, client=None, config=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client, config)


class PerformanceReportQuery(IncrementalTableStream):
    """
    Class Representing the `performance_report_query` Stream
    """

    tap_stream_id = "performance_report_query"
    key_properties = ["site_url", "search_type", "date", "query"]
    replication_key = "date"
    valid_replication_keys = ("date",)

    body_params = {'aggregationType': 'byProperty', 'dimensions': ['date', 'query']}

    def __init__(self, client=None, config=None) -> None:
        LOGGER.info("invoked %s", self.__class__)
        super().__init__(client, config)
