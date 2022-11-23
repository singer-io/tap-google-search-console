from tap_google_search_console import helpers
from unittest import TestCase


class TestHelpers(TestCase):

    def test_camel_case_conversion(self):
        """
        Tests the function which converts camel case to snake case
        """
        name = "TestCamelCase"
        self.assertEqual(helpers.convert(name), "test_camel_case")

    def test_convert_array(self):
        """
        Tests the function which converts all the nested dict type `KEYS` in list of items from camel to snake case
        """
        input_array = ["TestCase", {"TestCaseNumber": 22}, [{"TestSuite": [{"UnitTests": 23}]}]]
        expected_output = ['TestCase', {'test_case_number': 22}, [{'test_suite': [{'unit_tests': 23}]}]]
        self.assertEquals(helpers.convert_array(input_array), expected_output)

    def test_convert_json(self):
        """
        Tests the function which converts all the dict type `Keys` in a dict from camel to snake case
        """
        input_json = {"CamelCaseKey": "UnitTest", "SnakeCaseKeys": [{"first_name": "tester", "second_name": "dev"}]}
        expected_output = {'camel_case_key': 'UnitTest', 'snake_case_keys':
            [{'first_name': 'tester', 'second_name': 'dev'}]}
        self.assertEquals(helpers.convert_json(input_json), expected_output)

    def test_add_search_types(self):
        """
        Tests the function to add `search_type` Key to row in extracted data
        """
        input_data = {"rows": [{"app_name": "singer-io", "app_id": 124}, {"app_name": "", "app_id": 343}]}
        expected_output = {"rows": [{"app_name": "singer-io", "app_id": 124, "search_type": "app"},
                                    {"app_name": "", "app_id": 343, "search_type": "app"}]}
        self.assertEquals(helpers.add_site_url_search_type(input_data, "rows", "app",
                                                           key_name="search_type"), expected_output)

    def test_add_site_url(self):
        """
        Tests the function to add `search_type` Key to row in extracted data
        """
        input_data = {"rows": [{"app_name": "singer-io", "app_id": 124}, {"app_name": "", "app_id": 343}]}
        expected_output = {"rows": [{"app_name": "singer-io", "app_id": 124, "site_url": "https://www.test.com"},
                                    {"app_name": "", "app_id": 343, "site_url": "https://www.test.com"}]}
        self.assertEquals(helpers.add_site_url_search_type(input_data, "rows", "https://www.test.com"), expected_output)

