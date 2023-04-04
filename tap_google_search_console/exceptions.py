import json


class GoogleError(Exception):
    pass


class Server5xxError(GoogleError):
    pass


class GoogleBadRequestError(GoogleError):
    pass


class GoogleUnauthorizedError(GoogleError):
    pass


class GooglePaymentRequiredError(GoogleError):
    pass


class GoogleNotFoundError(GoogleError):
    pass


class GoogleMethodNotAllowedError(GoogleError):
    pass


class GoogleConflictError(GoogleError):
    pass


class GoogleGoneError(GoogleError):
    pass


class GooglePreconditionFailedError(GoogleError):
    pass


class GoogleRequestEntityTooLargeError(GoogleError):
    pass


class GoogleRequestedRangeNotSatisfiableError(GoogleError):
    pass


class GoogleExpectationFailedError(GoogleError):
    pass


class GoogleForbiddenError(GoogleError):
    pass


class GoogleUnprocessableEntityError(GoogleError):
    pass


class GooglePreconditionRequiredError(GoogleError):
    pass


class GoogleRateLimitExceeded(GoogleError):
    pass


class GoogleInternalServiceError(Server5xxError):
    pass


class GoogleNotImplementedError(Server5xxError):
    pass


class GoogleServiceUnavailable(Server5xxError):
    pass


class GoogleQuotaExceededError(GoogleError):
    pass


class GoogleInvalidGrant(GoogleError):
    pass


# Error Codes: https://developers.google.com/webmaster-tools/search-console-api-original/v3/errors
ERROR_CODE_EXCEPTION_MAPPING = {
    400: {"raise_exception": GoogleBadRequestError, "message": "The request is missing or has bad parameters."},
    401: {"raise_exception": GoogleUnauthorizedError, "message": "Invalid authorization credentials."},
    402: {
        "raise_exception": GooglePaymentRequiredError,
        "message": "The requested operation requires more resources than the quota allows. Payment is required to "
        "complete the operation.",
    },
    403: {"raise_exception": GoogleForbiddenError, "message": "Invalid authorization credentials or permissions."},
    404: {"raise_exception": GoogleNotFoundError, "message": "The requested resource does not exist."},
    405: {
        "raise_exception": GoogleMethodNotAllowedError,
        "message": "The HTTP method associated with the request is not supported.",
    },
    409: {
        "raise_exception": GoogleConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an "
        "existing item.",
    },
    410: {"raise_exception": GoogleGoneError, "message": "The requested resource is permanently unavailable."},
    412: {
        "raise_exception": GooglePreconditionFailedError,
        "message": "The condition set in the request's If-Match or If-None-Match HTTP request header was not met.",
    },
    413: {"raise_exception": GoogleRequestEntityTooLargeError, "message": "The request is too large."},
    416: {
        "raise_exception": GoogleRequestedRangeNotSatisfiableError,
        "message": "The request specified a range that cannot be satisfied.",
    },
    417: {
        "raise_exception": GoogleExpectationFailedError,
        "message": "A client expectation cannot be met by the server.",
    },
    422: {
        "raise_exception": GoogleUnprocessableEntityError,
        "message": "The request was not able to process right now.",
    },
    428: {
        "raise_exception": GooglePreconditionRequiredError,
        "message": "The request requires a precondition If-Match or If-None-Match which is not provided.",
    },
    429: {"raise_exception": GoogleRateLimitExceeded, "message": "Rate limit has been exceeded."},
    500: {"raise_exception": GoogleInternalServiceError, "message": "The request failed due to an internal error."},
    501: {"raise_exception": GoogleNotImplementedError, "message": "Functionality does not exist."},
    503: {"raise_exception": GoogleServiceUnavailable, "message": "The API service is currently unavailable."},
}


def raise_for_error(response):
    """Forming a response message for raising custom exception"""
    try:
        response_json = response.json()
    except Exception:
        response_json = {}

    error_code = response.status_code
    error_message = response_json.get("error_description") or response_json.get(
        "error", ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {})
    ).get("message", "An Unknown Error occurred,please try after some time.")

    message = f"HTTP-error-code: {error_code}, Error: {error_message}"

    # Raise GoogleQuotaExceededError if 403 error code returned due to QuotaExceeded
    response_error = json.dumps(response_json.get("error", error_message))
    if error_code == 403 and "quotaExceeded" in response_error:
        ex = GoogleQuotaExceededError
    elif error_code == 400 and "invalid_grant" in response_error:
        ex = GoogleInvalidGrant
        message = f"HTTP-error-code: {error_code}, Error: invalid_grant"
    else:
        ex = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", GoogleError)
    raise ex(message) from None
