import tap_google_search_console.client as client_
import unittest
import requests
from unittest import mock

def get_mock_http_response(status_code, contents):
    response = requests.Response()
    response.status_code = status_code
    response._content = contents.encode()
    return response

@mock.patch("requests.Session.request")
@mock.patch("tap_google_search_console.client.GoogleClient.get_access_token")
class TestCredentials(unittest.TestCase):

    def test_sites_access_valid(self, mocked_get_token, mocked_request):
        # Access token is valid and enough permission for site access
        mocked_request.return_value = get_mock_http_response(200, '{"success": {"code": 200}}')
        sites = "https://example.com, https://www.example.com"
        gsc_client = client_.GoogleClient('', '', '', sites, '')
        gsc_client.check_sites_access()
        self.assertEqual(mocked_request.call_count, 2)

    def test_sites_invalid_creds(self, mocked_get_token, mocked_request):
        # Access token is invalid for site query request
        mocked_request.return_value = get_mock_http_response(401, '{"error": {"code": 401}}')
        sites = "https://example.com, https://www.example.com"
        gsc_client = client_.GoogleClient('', '', '', sites, '')
        with self.assertRaises(client_.GoogleUnauthorizedError):
            gsc_client.check_sites_access()
        self.assertEqual(mocked_request.call_count, 1)

    def test_sites_access_forbidden(self, mocked_get_token, mocked_request):
        # Site is not valid or not accessible for user
        mocked_request.return_value = get_mock_http_response(403, '{"error": {"code": 403}}')
        sites = "https://example.com"
        gsc_client = client_.GoogleClient('', '', '', sites, '')
        with self.assertRaises(client_.GoogleForbiddenError):
            gsc_client.check_sites_access()
        self.assertEqual(mocked_request.call_count, 1)

@mock.patch("requests.Session.post")
@mock.patch("requests.Session.request")
class TestSiteAccessTokenScenario(unittest.TestCase):

    def test_sites_access_token_success(self, mocked_data_request, mocked_access_token_post_request):
        # Valid credentials for access token request
        mocked_access_token_post_request.return_value = get_mock_http_response(200, '{"access_token" : "abc", "expires_in" : 100}')
        mocked_data_request.return_value = get_mock_http_response(200, '{"success": {"code": 200}}')
        sites = "https://example.com"
        gsc_client = client_.GoogleClient('', '', '', sites, '')
        gsc_client.check_sites_access()
        self.assertEqual(mocked_data_request.call_count, 1)

    def test_sites_access_token_failed_401(self, mocked_data_request, mocked_access_token_post_request):
        # Invalid client id or secret for access token request
        mocked_access_token_post_request.return_value = get_mock_http_response(401, '{"error": {"code": 401}}')
        sites = "https://example.com"
        gsc_client = client_.GoogleClient('', '', '', sites, '')
        with self.assertRaises(client_.GoogleUnauthorizedError):
            gsc_client.check_sites_access()
        self.assertEqual(mocked_data_request.call_count, 0)

    def test_sites_access_token_failed_400(self, mocked_data_request, mocked_access_token_post_request):
        # Invalid refresh token for access token request
        mocked_access_token_post_request.return_value = get_mock_http_response(400, '{"error": {"code": 400}}')
        sites = "https://example.com"
        gsc_client = client_.GoogleClient('', '', '', sites, '')
        with self.assertRaises(client_.GoogleBadRequestError):
            gsc_client.check_sites_access()
        self.assertEqual(mocked_data_request.call_count, 0)

@mock.patch("tap_google_search_console.client.GoogleClient.post")
class TestSitesAccessCallCount(unittest.TestCase):

    def test_site_access_call_count(self, mocked_post_request):
        sites = "https://example.com, https://www.example.com, http://example.com, http://www.example.com, sc-domain:example.com"
        gsc_client = client_.GoogleClient('', '', '', sites, '')
        gsc_client.check_sites_access()
        self.assertEquals(mocked_post_request.call_count, 5)
