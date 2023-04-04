import unittest
from unittest import mock

import requests

import tap_google_search_console.client as client
from tap_google_search_console import exceptions


class MockResponse:
    def __init__(self, status_code, json, raise_error, text=None, content=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.content = content if content is not None else "google search console"

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        return self.text


def get_response(status_code, json=None, raise_error=False, content=None):
    if json is None:
        json = {}
    return MockResponse(status_code, json, raise_error, content=content)


@mock.patch("requests.Session.request")
@mock.patch("tap_google_search_console.client.GoogleClient.get_access_token")
class TestExceptionHandling(unittest.TestCase):
    def test_error_with_empty_response(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(400, raise_error=True, content="")
        google_client = client.GoogleClient("", "", "", "")
        with self.assertRaises(exceptions.GoogleError):
            google_client.request("")

    def test_400_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(400, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleBadRequestError as e:
            self.assertEqual(str(e), "HTTP-error-code: 400, Error: The request is missing or has bad parameters.")

    def test_401_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(401, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleUnauthorizedError as e:
            self.assertEqual(str(e), "HTTP-error-code: 401, Error: Invalid authorization credentials.")

    def test_401_error_in_response(self, mocked_access_token, mocked_request):
        json = {"error": "invalid_client", "error_description": "The OAuth client was not found."}
        mocked_request.return_value = get_response(401, json, True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleUnauthorizedError as e:
            self.assertEqual(str(e), f'HTTP-error-code: 401, Error: {json["error_description"]}')

    def test_402_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(402, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GooglePaymentRequiredError as e:
            self.assertEqual(
                str(e),
                "HTTP-error-code: 402, Error: The requested operation requires more resources "
                "than the quota allows. Payment is required to complete the operation.",
            )

    def test_403_normal_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(403, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleForbiddenError as e:
            self.assertEqual(str(e), "HTTP-error-code: 403, Error: Invalid authorization credentials or permissions.")

        # Normal 403 error so no retries
        self.assertEqual(mocked_request.call_count, 1)

    @mock.patch("time.sleep")
    def test_403_quota_exceeded_error(self, mocked_sleep, mocked_access_token, mocked_request):
        json = {
            "error": {
                "code": 403,
                "message": "Search Analytics load quota exceeded. Learn about usage limits: "
                "https://developers.google.com/webmaster-tools/v3/limits.",
                "errors": [
                    {
                        "message": "Search Analytics load quota exceeded. Learn about usage limits: "
                        "https://developers.google.com/webmaster-tools/v3/limits.",
                        "domain": "usageLimits",
                        "reason": "quotaExceeded",
                    }
                ],
            }
        }
        mocked_request.return_value = get_response(403, json, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleQuotaExceededError as e:
            self.assertEqual(str(e), f'HTTP-error-code: 403, Error: {json["error"]["message"]}')

        # QuotaExceeds 403 error so 2 retries
        self.assertEqual(mocked_request.call_count, 2)
        mocked_sleep.assert_called_with(900)

    def test_404_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(404, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleNotFoundError as e:
            self.assertEqual(str(e), "HTTP-error-code: 404, Error: The requested resource does not exist.")

    def test_404_error_in_response(self, mocked_access_token, mocked_request):
        json = {
            "error": {
                "code": 404,
                "message": "'https://demo310799.000webhostapp1.com' is not a verified Search Console site in this "
                "account.",
                "errors": [
                    {
                        "message": "'https://demo310799.000webhostapp1.com' is not a verified Search Console site in "
                        "this account.",
                        "domain": "global",
                        "reason": "notFound",
                        "location": "siteUrl",
                        "locationType": "parameter",
                    }
                ],
            }
        }
        mocked_request.return_value = get_response(404, json, True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleNotFoundError as e:
            self.assertEqual(str(e), f'HTTP-error-code: 404, Error: {json["error"]["message"]}')

    def test_405_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(405, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleMethodNotAllowedError as e:
            self.assertEqual(
                str(e), "HTTP-error-code: 405, Error: The HTTP method associated with the request is not " "supported."
            )

    def test_409_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(409, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleConflictError as e:
            self.assertEqual(
                str(e),
                "HTTP-error-code: 409, Error: The API request cannot be completed because the requested "
                "operation would conflict with an existing item.",
            )

    def test_410_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(410, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleGoneError as e:
            self.assertEqual(str(e), "HTTP-error-code: 410, Error: The requested resource is permanently unavailable.")

    def test_412_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(412, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GooglePreconditionFailedError as e:
            self.assertEqual(
                str(e),
                "HTTP-error-code: 412, Error: The condition set in the request's If-Match or "
                "If-None-Match HTTP request header was not met.",
            )

    def test_413_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(413, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleRequestEntityTooLargeError as e:
            self.assertEqual(str(e), "HTTP-error-code: 413, Error: The request is too large.")

    def test_416_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(416, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleRequestedRangeNotSatisfiableError as e:
            self.assertEqual(
                str(e), "HTTP-error-code: 416, Error: The request specified a range that cannot be satisfied."
            )

    def test_417_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(417, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleExpectationFailedError as e:
            self.assertEqual(str(e), "HTTP-error-code: 417, Error: A client expectation cannot be met by the server.")

    def test_422_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(422, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleUnprocessableEntityError as e:
            self.assertEqual(str(e), "HTTP-error-code: 422, Error: The request was not able to process right now.")

    def test_428_error(self, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(428, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GooglePreconditionRequiredError as e:
            self.assertEqual(
                str(e),
                "HTTP-error-code: 428, Error: The request requires a precondition If-Match or "
                "If-None-Match which is not provided.",
            )

    @mock.patch("time.sleep")
    def test_429_error(self, mocked_sleep, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(429, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleRateLimitExceeded as e:
            self.assertEqual(str(e), "HTTP-error-code: 429, Error: Rate limit has been exceeded.")

        # on 429 error function backoff and retries 7 times
        self.assertEqual(mocked_request.call_count, 7)

    @mock.patch("time.sleep")
    def test_500_error(self, mocked_sleep, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(500, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleInternalServiceError as e:
            self.assertEqual(str(e), "HTTP-error-code: 500, Error: The request failed due to an internal error.")

        # on 5xx error function backoff and retries 7 times
        self.assertEqual(mocked_request.call_count, 7)

    @mock.patch("time.sleep")
    def test_501_error(self, mocked_sleep, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(501, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleNotImplementedError as e:
            self.assertEqual(str(e), "HTTP-error-code: 501, Error: Functionality does not exist.")

        # on 5xx error function backoff and retries 7 times
        self.assertEqual(mocked_request.call_count, 7)

    @mock.patch("time.sleep")
    def test_503_error(self, mocked_sleep, mocked_access_token, mocked_request):
        mocked_request.return_value = get_response(503, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.request("")
        except exceptions.GoogleServiceUnavailable as e:
            self.assertEqual(str(e), "HTTP-error-code: 503, Error: The API service is currently unavailable.")

        # on 5xx error function backoff and retries 7 times
        self.assertEqual(mocked_request.call_count, 7)

    def test_200_success(self, mocked_access_token, mocked_request):
        json = {"key": "value", "tap": "google search console"}
        mocked_request.return_value = get_response(200, json)
        google_client = client.GoogleClient("", "", "", "")

        response = google_client.request("")
        self.assertEqual(json, response)


@mock.patch("requests.Session.post")
class TestAccessToken(unittest.TestCase):
    def test_error_with_empty_response(self, mocked_request):
        mocked_request.return_value = get_response(400, raise_error=True, content="")
        google_client = client.GoogleClient("", "", "", "")
        with self.assertRaises(exceptions.GoogleError):
            google_client.get_access_token()

    def test_400_error(self, mocked_request):
        mocked_request.return_value = get_response(400, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GoogleBadRequestError as e:
            self.assertEqual(str(e), "HTTP-error-code: 400, Error: The request is missing or has bad parameters.")

    def test_401_error(self, mocked_request):
        mocked_request.return_value = get_response(401, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GoogleUnauthorizedError as e:
            self.assertEqual(str(e), "HTTP-error-code: 401, Error: Invalid authorization credentials.")

    def test_402_error(self, mocked_request):
        mocked_request.return_value = get_response(402, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GooglePaymentRequiredError as e:
            self.assertEqual(
                str(e),
                "HTTP-error-code: 402, Error: The requested operation requires more resources "
                "than the quota allows. Payment is required to complete the operation.",
            )

    def test_403_error(self, mocked_request):
        mocked_request.return_value = get_response(403, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GoogleForbiddenError as e:
            self.assertEqual(str(e), "HTTP-error-code: 403, Error: Invalid authorization credentials or permissions.")

    def test_404_error(self, mocked_request):
        mocked_request.return_value = get_response(404, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GoogleNotFoundError as e:
            self.assertEqual(str(e), "HTTP-error-code: 404, Error: The requested resource does not exist.")

    def test_405_error(self, mocked_request):
        mocked_request.return_value = get_response(405, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GoogleMethodNotAllowedError as e:
            self.assertEqual(
                str(e), "HTTP-error-code: 405, Error: The HTTP method associated with the request is " "not supported."
            )

    def test_409_error(self, mocked_request):
        mocked_request.return_value = get_response(409, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GoogleConflictError as e:
            self.assertEqual(
                str(e),
                "HTTP-error-code: 409, Error: The API request cannot be completed because the "
                "requested operation would conflict with an existing item.",
            )

    def test_410_error(self, mocked_request):
        mocked_request.return_value = get_response(410, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GoogleGoneError as e:
            self.assertEqual(str(e), "HTTP-error-code: 410, Error: The requested resource is permanently unavailable.")

    def test_412_error(self, mocked_request):
        mocked_request.return_value = get_response(412, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GooglePreconditionFailedError as e:
            self.assertEqual(
                str(e),
                "HTTP-error-code: 412, Error: The condition set in the request's If-Match or "
                "If-None-Match HTTP request header was not met.",
            )

    def test_413_error(self, mocked_request):
        mocked_request.return_value = get_response(413, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GoogleRequestEntityTooLargeError as e:
            self.assertEqual(str(e), "HTTP-error-code: 413, Error: The request is too large.")

    def test_416_error(self, mocked_request):
        mocked_request.return_value = get_response(416, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GoogleRequestedRangeNotSatisfiableError as e:
            self.assertEqual(
                str(e), "HTTP-error-code: 416, Error: The request specified a range that cannot be " "satisfied."
            )

    def test_417_error(self, mocked_request):
        mocked_request.return_value = get_response(417, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GoogleExpectationFailedError as e:
            self.assertEqual(str(e), "HTTP-error-code: 417, Error: A client expectation cannot be met by the server.")

    def test_422_error(self, mocked_request):
        mocked_request.return_value = get_response(422, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GoogleUnprocessableEntityError as e:
            self.assertEqual(str(e), "HTTP-error-code: 422, Error: The request was not able to process right now.")

    def test_428_error(self, mocked_request):
        mocked_request.return_value = get_response(428, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except exceptions.GooglePreconditionRequiredError as e:
            self.assertEqual(
                str(e),
                "HTTP-error-code: 428, Error: The request requires a precondition If-Match or "
                "If-None-Match which is not provided.",
            )

    def test_429_error(self, mocked_request):
        mocked_request.return_value = get_response(429, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        try:
            google_client.get_access_token()
        except client.GoogleRateLimitExceeded as e:
            self.assertEqual(str(e), "HTTP-error-code: 429, Error: Rate limit has been exceeded.")

    @mock.patch("time.sleep")
    def test_500_error(self, mocked_sleep, mocked_request):
        mocked_request.return_value = get_response(500, raise_error=True)
        with self.assertRaises(exceptions.GoogleInternalServiceError) as err:
            with client.GoogleClient("", "", "", "") as _:
                self.assertEqual(str(err), "HTTP-error-code: 500, Error: The request failed due to an internal error.")
            # on 5xx error function backoff and retries 5 times
            self.assertEqual(mocked_request.call_count, 5)

    @mock.patch("time.sleep")
    def test_501_error(self, mocked_sleep, mocked_request):
        mocked_request.return_value = get_response(501, raise_error=True)
        with self.assertRaises(exceptions.GoogleNotImplementedError) as err:
            with client.GoogleClient("", "", "", "") as _:
                self.assertEqual(str(err), "HTTP-error-code: 501, Error: Functionality does not exist.")
            # on 5xx error function backoff and retries 5 times
            self.assertEqual(mocked_request.call_count, 5)

    @mock.patch("time.sleep")
    def test_503_error(self, mocked_sleep, mocked_request):
        mocked_request.return_value = get_response(503, raise_error=True)
        google_client = client.GoogleClient("", "", "", "")
        with self.assertRaises(exceptions.GoogleServiceUnavailable) as err:
            google_client.get_access_token()
            self.assertEqual(str(err), "HTTP-error-code: 503, Error: The API service is currently unavailable.")

            # on 5xx error function backoff and retries 5 times
            self.assertEqual(mocked_request.call_count, 5)

    @mock.patch("tap_google_search_console.client.LOGGER.info")
    def test_200_success(self, mocked_logger, mocked_request):
        json = {"access_token": "googlesearchconsole", "expires_in": 1}
        mocked_request.return_value = get_response(200, json=json)
        google_client = client.GoogleClient("", "", "", "")

        google_client.get_access_token()
        self.assertEqual(mocked_logger.call_count, 1)
