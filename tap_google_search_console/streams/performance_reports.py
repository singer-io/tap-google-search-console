from singer.logger import get_logger

from .abstract import IncrementalTableStream

LOGGER = get_logger()


class PerformanceReportCustom(IncrementalTableStream):
    """Class Representing the `performance_report_custom` Stream."""

    tap_stream_id = "performance_report_custom"
    key_properties = ["site_url", "search_type", "date", "dimensions_hash_key"]
    valid_replication_keys = ("date",)

    body_params = {"aggregationType": "auto"}
    dimension_list = ["date", "country", "device", "page", "query"]


class PerformanceReportDate(IncrementalTableStream):
    """Class Representing the `performance_report_date` Stream."""

    tap_stream_id = "performance_report_date"
    key_properties = ["site_url", "search_type", "date"]
    valid_replication_keys = ("date",)

    body_params = {"aggregationType": "byProperty", "dimensions": ["date"]}


class PerformanceReportCountry(IncrementalTableStream):
    """Class Representing the `performance_report_country` Stream."""

    tap_stream_id = "performance_report_country"
    key_properties = ["site_url", "search_type", "date", "country"]
    valid_replication_keys = ("date",)

    body_params = {"aggregationType": "byProperty", "dimensions": ["date", "country"]}


class PerformanceReportDevices(IncrementalTableStream):
    """Class Representing the `performance_report_device` Stream."""

    tap_stream_id = "performance_report_device"
    key_properties = ["site_url", "search_type", "date", "device"]
    # Excluding discover sub_type since Requests for Discover cannot be grouped by device.
    # this also overwrites the sub_types attribute declared for class IncrementalTableStream
    # in abstract.py
    sub_types = ["googleNews", "image", "news", "video", "web"]
    valid_replication_keys = ("date",)

    body_params = {"aggregationType": "byProperty", "dimensions": ["date", "device"]}


class PerformanceReportPage(IncrementalTableStream):
    """Class Representing the `performance_report_page` Stream."""

    tap_stream_id = "performance_report_page"
    key_properties = ["site_url", "search_type", "date", "page"]
    valid_replication_keys = ("date", "page")

    body_params = {"aggregationType": "byPage", "dimensions": ["date", "page"]}


class PerformanceReportQuery(IncrementalTableStream):
    """Class Representing the `performance_report_query` Stream."""

    tap_stream_id = "performance_report_query"
    key_properties = ["site_url", "search_type", "date", "query"]
    # Excluding discover and googleNews since query seems to be an invalid argument while
    # grouping data for discover and googleNews
    # this also overwrites the sub_types attribute declared for class IncrementalTableStream
    # in abstract.py
    sub_types = ["image", "news", "video", "web"]
    valid_replication_keys = ("date",)

    body_params = {"aggregationType": "byProperty", "dimensions": ["date", "query"]}
