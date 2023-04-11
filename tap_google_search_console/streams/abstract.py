import json
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Union

from singer import (
    Transformer,
    metadata,
    metrics,
    should_sync_field,
    utils,
    write_record,
    write_state,
)
from singer.logger import get_logger
from singer.metadata import get_standard_metadata

from tap_google_search_console.helpers import encode_and_format_url, transform_json

LOGGER = get_logger()


class BaseStream(ABC):
    """Base class representing generic stream methods and meta-attributes."""

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def forced_replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_key(self) -> str:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def valid_replication_keys(self) -> Tuple[str, str]:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """The unique identifier for the stream.

        This is allowed to be different from the name of the stream in
        order to allow for sources that have duplicate stream names.
        """

    @abstractmethod
    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict):
        """Performs Sync."""

    def __init__(self, client=None, config=None) -> None:
        self.client = client
        self.config = config

    @classmethod
    def get_metadata(cls, schema) -> Dict[str, str]:
        """Returns a `dict` for generating stream metadata."""
        return get_standard_metadata(**{
            "schema": schema,
            "key_properties": cls.key_properties,
            "valid_replication_keys": cls.valid_replication_keys,
            "replication_method": cls.replication_method or cls.forced_replication_method,
        }
                                         )

    def get_site_url(self):
        return self.config.get("site_urls", "").replace(" ", "").split(",")


class IncrementalTableStream(BaseStream, ABC):
    """Base Class for Incremental Stream."""

    replication_method = "INCREMENTAL"
    forced_replication_method = "INCREMENTAL"
    replication_key = "date"
    pagination = "body"
    sub_types = ["discover", "googleNews", "image", "news", "video", "web"]
    row_limit = 10000
    path = "sites/{}/searchAnalytics/query"
    data_key = "rows"
    now_dt_tm = utils.now()
    dimension_list = []
    body_params = {}

    # declaring this variable to keep track of number of
    # records processed per stream, per site, per sub_type
    records_extracted = 0

    @staticmethod
    def get_bookmark(state: Dict, stream: str, site: str, sub_type: str, default: str) -> str:
        """Fetches the bookmark from the state file for a given stream, site,
        sub_type."""

        if (state is None) or ("bookmarks" not in state):
            return default
        return state.get("bookmarks", {}).get(stream, {}).get(site, {}).get(sub_type, default)

    @property
    def get_attribution_days(self) -> Union[str, int]:
        """Sets this value to 4days since there is data delay of 2-3 days from GSC"""
        return int(self.config.get("ATTRIBUTION_DAYS") or 4)

    @property
    def get_date_window_size(self) -> Union[str, int]:
        return int(self.config.get("DATE_WINDOW_SIZE") or 30)

    def write_bookmark(self, state: Dict, site: str, sub_type: str, value: str) -> None:
        """Writes bookmark to state file for a given stream, site, sub_type."""
        if "bookmarks" not in state:
            state["bookmarks"] = {}
        if self.tap_stream_id not in state["bookmarks"]:
            state["bookmarks"][self.tap_stream_id] = {}
        if site not in state["bookmarks"][self.tap_stream_id]:
            state["bookmarks"][self.tap_stream_id][site] = {}
        state["bookmarks"][self.tap_stream_id][site][sub_type] = value
        LOGGER.info(f"Write state for Stream: {self.tap_stream_id}, Site: {site}, Type: {sub_type}, value: {value}")
        write_state(state)

    def set_start_and_end_times(self, state: Dict, stream: str, sub_type: str, site: str) -> Tuple[datetime, datetime]:
        """Method to set start and end times."""
        # get the bookmark from state file
        report_bookmark = self.get_bookmark(state, stream, site, sub_type, self.config.get("start_date"))
        reports_dt_tm = utils.strptime_to_utc(report_bookmark)

        delta_days = (self.now_dt_tm - reports_dt_tm).days

        if delta_days < self.get_attribution_days:
            start_dt_tm = self.now_dt_tm - timedelta(days=self.get_attribution_days)
        else:
            start_dt_tm = reports_dt_tm

        end_dt_tm = start_dt_tm + timedelta(days=self.get_date_window_size)
        if end_dt_tm > self.now_dt_tm:
            end_dt_tm = self.now_dt_tm

        return start_dt_tm, end_dt_tm

    def set_dimensions_in_payload(self, stream_metadata: Dict) -> List:
        """Set only the selected (field selection) dimensions in API
        payload."""
        selected_dimensions = []
        for dimension in self.dimension_list:
            if should_sync_field(
                metadata.get(stream_metadata, ("properties", dimension), "inclusion"),
                metadata.get(stream_metadata, ("properties", dimension), "selected"),
            ):
                selected_dimensions.append(dimension)
        return selected_dimensions

    def make_payload(self, sub_type: str, start_date: str, end_date: str, stream_metadata: Dict) -> Dict:
        """Creates payload for POST API Call."""
        if self.tap_stream_id == "performance_report_custom":
            self.body_params["dimensions"] = self.set_dimensions_in_payload(stream_metadata)
            # Remove discover dimension from dimension_list if sub_type is discover
            # Requests for Discover cannot be grouped by device
            if sub_type == "discover" and "device" in self.body_params["dimensions"]:
                LOGGER.info(f"Removing the device dimension/field since it is incompatible with"
                            f" {sub_type} sub_type for custom report")
                self.body_params["dimensions"].remove("device")
        if sub_type in {"discover", "googleNews"}:
            self.body_params["aggregationType"] = "auto"
            # Remove query from dimension list if the sub_type is either discover or googleNews
            # query seems to be an invalid argument while grouping data for discover and googleNews
            if self.tap_stream_id == "performance_report_custom" and \
                    "query" in self.body_params["dimensions"]:
                LOGGER.info(f"Removing the query dimension/field since it is incompatible with"
                            f" {sub_type} sub_type for custom report")
                self.body_params["dimensions"].remove("query")

        return {"type": sub_type, "startDate": start_date, "endDate": end_date, **self.body_params}

    def validate_keys_in_data(self, extracted_data: List) -> None:
        """Validates the data by checking the primary keys in extracted
        data."""
        for record in extracted_data:
            for key in self.key_properties:
                if not record.get(key):
                    primary_keys_only = {id_field: record.get(id_field) for id_field in self.key_properties}
                    raise ValueError(f"Missing key {key} in record with primary keys {primary_keys_only}")

    def modify_start_end_dt_tm(self, end_dt_tm: datetime) -> Tuple[datetime, datetime]:
        """Sets start_date_time of a new window to end_date_time of old window
        Sets end_date_time of new window to 30 days(date_window_size) ahead
        since start_date_time."""
        start_dt_tm = end_dt_tm
        end_dt_tm = start_dt_tm + timedelta(days=self.get_date_window_size)
        if end_dt_tm > self.now_dt_tm:
            end_dt_tm = self.now_dt_tm
        return start_dt_tm, end_dt_tm

    def process_records(
        self,
        schema: Dict,
        stream_metadata: Dict,
        records: List,
        time_extracted: datetime,
        max_bookmark_value=None,
        last_datetime=None,
    ) -> str:
        """Filters out the unselected fields by the user Picks the latest
        bookmark value from extracted data Writes the records to stdout."""

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in records:
                # Transform record for Singer.io
                with Transformer() as transformer:
                    transformed_record = transformer.transform(record, schema, stream_metadata)

                    # Reset max_bookmark_value to new value if higher
                    if self.replication_key in transformed_record:
                        bookmark_date = transformed_record.get(self.replication_key)
                        bookmark_dt_tm = utils.strptime_to_utc(bookmark_date)
                        last_dt_tm = utils.strptime_to_utc(last_datetime)

                        if not max_bookmark_value:
                            max_bookmark_value = last_datetime

                        max_bookmark_dt_tm = utils.strptime_to_utc(max_bookmark_value)

                        if bookmark_dt_tm > max_bookmark_dt_tm:
                            max_bookmark_value = utils.strftime(bookmark_dt_tm)

                        # Keep only records whose bookmark is after the last_datetime
                        if bookmark_dt_tm >= last_dt_tm:
                            write_record(self.tap_stream_id, transformed_record, time_extracted=time_extracted)
                            counter.increment()
                    else:
                        write_record(self.tap_stream_id, transformed_record, time_extracted=time_extracted)
                        counter.increment()

            LOGGER.info(f"Stream: {self.tap_stream_id}, Processed {counter.value} records")
            self.records_extracted += counter.value
            return max_bookmark_value

    def get_records_for_sub_type(
        self, site_url: str, sub_type: str, state: Dict, schema: Dict, stream_metadata: Dict
    ) -> None:
        """Sync the data for a given sub_type, stream, site Gets the bookmark
        value or start date value, extracts data for date window size of 30
        days."""
        start_dt_tm, end_dt_tm = self.set_start_and_end_times(state, self.tap_stream_id, sub_type, site_url)
        LOGGER.info(f"bookmark value or start date for {self.tap_stream_id} {site_url} {sub_type}: {start_dt_tm}")
        site_path = encode_and_format_url(site_url, self.path)
        while start_dt_tm < end_dt_tm:
            offset, row_limit, batch_count = 0, self.row_limit, self.row_limit
            last_datetime = self.get_bookmark(
                state, self.tap_stream_id, site_url, sub_type, self.config.get("start_date")
            )
            bookmark_value = last_datetime
            start_str, end_str = utils.strftime(start_dt_tm)[:10], utils.strftime(end_dt_tm)[:10]

            LOGGER.info(
                f"Running sync for {site_url}, {self.tap_stream_id}, {sub_type} between date window "
                f"{start_str} {end_str}"
            )
            payload = self.make_payload(sub_type, start_str, end_str, stream_metadata)
            while row_limit == batch_count:
                body = {"startRow": offset, "rowLimit": row_limit, **payload}
                time_extracted = utils.now()
                LOGGER.info(f"body = {body}")
                data = self.client.post(site_path, endpoint=self.tap_stream_id, data=json.dumps(body))
                if not data:
                    self.write_bookmark(state, site_url, sub_type, bookmark_value)
                    LOGGER.info(f"There are no raw data records for date window {start_dt_tm} to {end_dt_tm}, "
                                f" from offset value {offset}")
                    start_dt_tm, end_dt_tm = self.modify_start_end_dt_tm(end_dt_tm)
                transformed_data = []
                if self.data_key in data:
                    transformed_data = transform_json(
                        data,
                        self.tap_stream_id,
                        self.data_key,
                        site_url,
                        sub_type,
                        dimensions_list=payload.get("dimensions", []),
                    )[self.data_key]

                if not transformed_data:
                    self.write_bookmark(state, site_url, sub_type, bookmark_value)
                    start_dt_tm, end_dt_tm = self.modify_start_end_dt_tm(end_dt_tm)

                self.validate_keys_in_data(transformed_data)
                LOGGER.info(f"Total synced records for {sub_type} {self.tap_stream_id}: {len(transformed_data)}")
                batch_count = len(transformed_data)
                bookmark_value = self.process_records(
                    schema,
                    stream_metadata,
                    transformed_data,
                    time_extracted,
                    bookmark_value,
                    last_datetime=last_datetime,
                )
                self.write_bookmark(state, site_url, sub_type, bookmark_value)
                offset = offset + row_limit

            start_dt_tm, end_dt_tm = self.modify_start_end_dt_tm(end_dt_tm)

    def get_records_for_site(self, site_url: str, state: Dict, schema: Dict, stream_metadata: Dict) -> None:
        """Starts Syncing data for each sub_type for a given site Logs the
        total number of extracted records."""
        for sub_type in self.sub_types:
            LOGGER.info(f"Starting Sync for Stream {self.tap_stream_id}, Site {site_url}, Type {sub_type}")
            self.records_extracted = 0
            self.get_records_for_sub_type(site_url, sub_type, state, schema, stream_metadata)
            LOGGER.info(
                f"Total records extracted for Stream: {self.tap_stream_id}, Site: {site_url}, Type: {sub_type}:"
                f" {self.records_extracted}"
            )
            LOGGER.info(f"Finished Sync for Stream {self.tap_stream_id}, Site {site_url}, Type {sub_type}")

    def get_records(self, state: Dict, schema: Dict, stream_metadata: Dict) -> None:
        """starts extracting data for each site_url configured by the user."""
        for site in self.get_site_url():
            LOGGER.info(f"Starting Sync for Stream {self.tap_stream_id}, Site {site}")
            self.get_records_for_site(site, state, schema, stream_metadata)
            LOGGER.info(f"Finished Sync for Stream {self.tap_stream_id}, Site {site}")

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict) -> None:
        """Starts Sync."""
        LOGGER.info(f"Starting Sync for Stream {self.tap_stream_id}")
        self.get_records(state, schema, stream_metadata)
        LOGGER.info(f"Finished Sync for Stream {self.tap_stream_id}")


class FullTableStream(BaseStream, ABC):
    """Base Class for Incremental Stream."""

    replication_method = "FULL_TABLE"
    forced_replication_method = "FULL_TABLE"
    api_method = "GET"
    valid_replication_keys = None
    replication_key = None

    @abstractmethod
    def get_records(self):
        """Extracts Records."""

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict):
        LOGGER.info("sync called from %s", self.__class__)
        with metrics.record_counter(self.tap_stream_id) as counter:
            time_extracted = utils.now()
            for record in self.get_records():
                with Transformer() as transformer:
                    transformed_record = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed_record, time_extracted=time_extracted)
                    counter.increment()
