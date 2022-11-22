from .performance_reports import (
    PerformanceReportCountry,
    PerformanceReportCustom,
    PerformanceReportDate,
    PerformanceReportDevices,
    PerformanceReportPage,
    PerformanceReportQuery,
)
from .sitemaps import Sitemaps
from .sites import Sites

STREAMS = {
    Sites.tap_stream_id: Sites,
    Sitemaps.tap_stream_id: Sitemaps,
    PerformanceReportCustom.tap_stream_id: PerformanceReportCustom,
    PerformanceReportDate.tap_stream_id: PerformanceReportDate,
    PerformanceReportCountry.tap_stream_id: PerformanceReportCountry,
    PerformanceReportDevices.tap_stream_id: PerformanceReportDevices,
    PerformanceReportPage.tap_stream_id: PerformanceReportPage,
    PerformanceReportQuery.tap_stream_id: PerformanceReportQuery,
}


# "performance_report_custom": PerformanceReportCustom,
# "performance_report_date": PerformanceReportDate,
# "performance_report_country": PerformanceReportCountry,
# "performance_report_device": PerformanceReportDevices,
# "performance_report_page": PerformanceReportPage,
# "performance_report_query": PerformanceReportQuery,
