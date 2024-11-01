import unittest
from pandas.core.frame import DataFrame
import get_currency_rate


class Test_Get_Currency_Rate(unittest.TestCase):
    # test if we recieve data from API
    def test_get_currency_api_data(self):
        data = get_currency_rate.get_currency_api_data()
        self.assertFalse(data.empty)

    # test if db connection is working
    def test_connect_to_sqlLite(self):
        conn = get_currency_rate.connect_to_sqlLite()
        self.assertNotEqual(conn, None)


if __name__ == "__main__":
    unittest.main()
