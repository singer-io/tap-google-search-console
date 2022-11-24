import contextlib
import unittest
from unittest import mock

import requests

import tap_google_search_console.client as client


class TestTimeoutValue(unittest.TestCase):
    """Verify the value of timeout is set as expected."""

    def test_timeout_value_not_passed_in_config(self):
        config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
            "site_urls": "www.test.com",
            "start_date": "2021-01-01T00:00:00Z",
            "user_agent": "test_user_agent",
        }

        # initialize 'GoogleClient'
        cl = client.GoogleClient(
            config["client_id"],
            config["client_secret"],
            config["refresh_token"],
            config["site_urls"],
            user_agent=config["user_agent"],
            timeout=config.get("request_timeout"),
        )

        # verify that timeout value is default as request timeout is not passed in config
        self.assertEqual(300, cl.request_timeout)

    def test_timeout_int_value_passed_in_config(self):
        config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
            "site_urls": "www.test.com",
            "start_date": "2021-01-01T00:00:00Z",
            "user_agent": "test_user_agent",
            "request_timeout": 100,
        }

        # initialize 'GoogleClient'
        cl = client.GoogleClient(
            config["client_id"],
            config["client_secret"],
            config["refresh_token"],
            config["site_urls"],
            user_agent=config["user_agent"],
            timeout=config.get("request_timeout"),
        )

        # verify that timeout value is same as the value passed in the config
        self.assertEqual(100.0, cl.request_timeout)

    def test_timeout_string_value_passed_in_config(self):
        config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
            "site_urls": "www.test.com",
            "start_date": "2021-01-01T00:00:00Z",
            "user_agent": "test_user_agent",
            "request_timeout": "100",
        }

        # initialize 'GoogleClient'
        cl = client.GoogleClient(
            config["client_id"],
            config["client_secret"],
            config["refresh_token"],
            config["site_urls"],
            user_agent=config["user_agent"],
            timeout=config.get("request_timeout"),
        )

        # verify that timeout value is same as the value passed in the config
        self.assertEqual(100.0, cl.request_timeout)

    def test_timeout_empty_value_passed_in_config(self):
        config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
            "site_urls": "www.test.com",
            "start_date": "2021-01-01T00:00:00Z",
            "user_agent": "test_user_agent",
            "request_timeout": "",
        }

        # initialize 'GoogleClient'
        cl = client.GoogleClient(
            config["client_id"],
            config["client_secret"],
            config["refresh_token"],
            config["site_urls"],
            user_agent=config["user_agent"],
            timeout=config.get("request_timeout"),
        )

        # verify that timeout value is default as request timeout is empty in the config
        self.assertEqual(300, cl.request_timeout)

    def test_timeout_0_value_passed_in_config(self):
        config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
            "site_urls": "www.test.com",
            "start_date": "2021-01-01T00:00:00Z",
            "user_agent": "test_user_agent",
            "request_timeout": 0.0,
        }

        # initialize 'GoogleClient'
        cl = client.GoogleClient(
            config["client_id"],
            config["client_secret"],
            config["refresh_token"],
            config["site_urls"],
            user_agent=config["user_agent"],
            timeout=config.get("request_timeout"),
        )

        # verify that timeout value is default as request timeout is zero in the config
        self.assertEqual(300, cl.request_timeout)

    def test_timeout_string_0_value_passed_in_config(self):
        config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
            "site_urls": "www.test.com",
            "start_date": "2021-01-01T00:00:00Z",
            "user_agent": "test_user_agent",
            "request_timeout": "0.0",
        }

        # initialize 'GoogleClient'
        cl = client.GoogleClient(
            config["client_id"],
            config["client_secret"],
            config["refresh_token"],
            config["site_urls"],
            user_agent=config["user_agent"],
            timeout=config.get("request_timeout"),
        )

        # verify that timeout value is default as request timeout is zero in the config
        self.assertEqual(300, cl.request_timeout)


@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
class TestTimeoutBackoff(unittest.TestCase):
    """Verify that we backoff for 5 times for the 'Timeout' error."""

    def test_timeout_error__get_access_token(self, mocked_request, mocked_sleep):
        # mock request and raise the 'Timeout' error
        mocked_request.side_effect = requests.Timeout

        config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
            "site_urls": "www.test.com",
            "start_date": "2021-01-01T00:00:00Z",
            "user_agent": "test_user_agent",
        }

        # initialize 'GoogleClient'
        with contextlib.suppress(requests.Timeout):
            cl = client.GoogleClient(
                config["client_id"],
                config["client_secret"],
                config["refresh_token"],
                config["site_urls"],
                user_agent=config["user_agent"],
                timeout=config.get("request_timeout"),
            )
            cl.request("/path")

        # verify that we backoff for 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_timeout_error__request(self, mocked_request, mocked_sleep):
        # mock request and raise the 'Timeout' error
        mocked_request.side_effect = requests.Timeout

        config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
            "site_urls": "www.test.com",
            "start_date": "2021-01-01T00:00:00Z",
            "user_agent": "test_user_agent",
        }

        # initialize 'GoogleClient'
        cl = client.GoogleClient(
            config["client_id"],
            config["client_secret"],
            config["refresh_token"],
            config["site_urls"],
            user_agent=config["user_agent"],
            timeout=config.get("request_timeout"),
        )

        with contextlib.suppress(requests.Timeout):
            # function call
            cl.request("GET")
        # verify that we backoff for 5 times
        self.assertEqual(mocked_request.call_count, 5)


@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
class TestConnectionErrorBackoff(unittest.TestCase):
    """Verify that we backoff for 5 times for the 'ConnectionError' error."""

    def test_connection_error__get_access_token(self, mocked_request, mocked_sleep):
        # mock request and raise the 'ConnectionError' error
        mocked_request.side_effect = requests.ConnectionError

        config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
            "site_urls": "www.test.com",
            "start_date": "2021-01-01T00:00:00Z",
            "user_agent": "test_user_agent",
        }

        # initialize 'GoogleClient'
        with contextlib.suppress(requests.ConnectionError):
            cl = client.GoogleClient(
                config["client_id"],
                config["client_secret"],
                config["refresh_token"],
                config["site_urls"],
                user_agent=config["user_agent"],
                timeout=config.get("request_timeout"),
            )
            cl.request("GET")
        # verify that we backoff for 7 times
        self.assertEqual(mocked_request.call_count, 7)
