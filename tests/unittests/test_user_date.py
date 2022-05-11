import unittest
from tap_jira.streams import transform_user_date

TEST_SET = {
    "12/okt/2022": "2022-10-12",
    "02/abr/2021": "2021-04-02"
}
class TestUserDateTransform(unittest.TestCase):
    """
    Verify that tap successfully transform date value of different regional languages.
    """
    def test_user_date_for_any_region(self):

        for actual_test_date, expected_test_date in TEST_SET.items():
            self.assertEqual(transform_user_date(actual_test_date), expected_test_date)

