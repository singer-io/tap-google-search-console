import json
from unittest import TestCase
from unittest.mock import Mock, patch

from singer import utils

from tap_google_search_console.streams.performance_reports import PerformanceReportPage


class TestEmptyDateWindows(TestCase):
    def test_empty_windows_do_not_skip_following_date_windows(self):
        stream = PerformanceReportPage(
            client=Mock(),
            config={"start_date": "2024-01-01T00:00:00Z", "DATE_WINDOW_SIZE": 2},
        )
        stream.now_dt_tm = utils.strptime_to_utc("2024-01-08T00:00:00Z")
        stream.row_limit = 2
        stream.client.post.side_effect = [
            {},
            {"rows": []},
            {"rows": [{"keys": ["2024-01-05", "/page-1"]}, {"keys": ["2024-01-06", "/page-2"]}]},
            {"rows": [{"keys": ["2024-01-06", "/page-3"]}]},
            {"rows": [{"keys": ["2024-01-07", "/page-4"]}]},
        ]

        with patch.object(stream, "write_bookmark"), patch.object(
            stream, "process_records", side_effect=lambda *args, **kwargs: args[4]
        ):
            stream.get_records_for_sub_type("https://example.com", "web", {}, {}, {})

        requested_bodies = [json.loads(call.kwargs["data"]) for call in stream.client.post.call_args_list]
        requested_windows = [
            (body["startDate"], body["endDate"], body["startRow"]) for body in requested_bodies
        ]
        self.assertEqual(
            requested_windows,
            [
                ("2024-01-01", "2024-01-03", 0),
                ("2024-01-03", "2024-01-05", 0),
                ("2024-01-05", "2024-01-07", 0),
                ("2024-01-05", "2024-01-07", 2),
                ("2024-01-07", "2024-01-08", 0),
            ],
        )
