import json
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import backoff
import requests
import singer
from requests.exceptions import ConnectionError, Timeout
from singer import metrics, utils

from .exceptions import (
    GoogleQuotaExceededError,
    GoogleRateLimitExceeded,
    Server5xxError,
    raise_for_error,
)

BASE_URL = "https://www.googleapis.com/webmasters/v3"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
LOGGER = singer.get_logger()

# set default timeout of 300 seconds
REQUEST_TIMEOUT = 300


class GoogleClient:  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        site_urls: str,
        user_agent=None,
        timeout=REQUEST_TIMEOUT,
    ):

        self.__client_id, self.__client_secret, self.__refresh_token = (client_id, client_secret, refresh_token)
        self.__site_urls, self.__user_agent = site_urls, user_agent
        self.__access_token, self.__expires, self.base_url = None, None, None
        self.__session = requests.Session()

        try:
            self.request_timeout = REQUEST_TIMEOUT if timeout in (None, 0, "0", "0.0") else float(timeout)
        except ValueError:
            self.request_timeout = REQUEST_TIMEOUT

    def check_sites_access(self) -> None:
        """Perform access check for each site url provided."""
        body = json.dumps({"startDate": "2021-04-01", "endDate": "2021-05-01"})
        for site_url in self.__site_urls.replace(" ", "").split(","):
            self.post(f"sites/{quote(site_url, safe='')}/searchAnalytics/query", data=body)

    @backoff.on_exception(backoff.expo, (Server5xxError, ConnectionError, Timeout), max_tries=5, factor=2)
    def __enter__(self):
        self.get_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    def get_access_token(self) -> None:
        """Performs authentication and the access token if expired."""

        if self.__access_token and self.__expires > datetime.now(timezone.utc):
            return
        headers = {"User-Agent": self.__user_agent or ""}
        response = self.__session.post(
            url=GOOGLE_TOKEN_URI,
            headers=headers,
            data={
                "grant_type": "refresh_token",
                "client_id": self.__client_id,
                "client_secret": self.__client_secret,
                "refresh_token": self.__refresh_token,
            },
            timeout=self.request_timeout,
        )

        if response.status_code != 200:
            raise_for_error(response)

        data = response.json()
        self.__access_token = data["access_token"]
        self.__expires = utils.now() + timedelta(seconds=data["expires_in"])

        LOGGER.info("Authorized, token expires = %s", self.__expires)

    # Backoff for 15 minutes in case of Quota Exceeded error
    @backoff.on_exception(backoff.constant, GoogleQuotaExceededError, max_tries=2, interval=900, jitter=None)
    # backoff for 5 times, with 10 seconds consistent interval
    @backoff.on_exception(backoff.constant, Timeout, max_tries=5, interval=10, jitter=None)
    @backoff.on_exception(
        backoff.expo, (Server5xxError, ConnectionError, GoogleRateLimitExceeded), max_tries=7, factor=3
    )
    @utils.ratelimit(1200, 60)
    def request(self, method: str, path: str = None, url: str = None, **kwargs) -> Any:
        """Wrapper method around request.sessions get/post method using the
        session object of the GoogleClient Object."""

        # TODO: Consolidate multiple backoff decorators
        self.get_access_token()
        url = url or f"{self.base_url or BASE_URL}/{path}"

        endpoint, kwargs["headers"] = kwargs.get("endpoint", None), kwargs.get("headers", {})
        kwargs.pop("endpoint", None)

        kwargs["headers"]["Authorization"] = f"Bearer {self.__access_token}"
        if self.__user_agent:
            kwargs["headers"]["User-Agent"] = self.__user_agent
        if method == "POST":
            kwargs["headers"]["Content-Type"] = "application/json"

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, timeout=self.request_timeout, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code != 200:
            raise_for_error(response)

        return response.json()

    def get(self, path: str, **kwargs) -> Any:
        """wrapper for get method."""
        return self.request("GET", path=path, **kwargs)

    def post(self, path: str, **kwargs) -> Any:
        """wrapper for post method."""
        return self.request("POST", path=path, **kwargs)
