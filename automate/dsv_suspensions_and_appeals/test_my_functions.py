import unittest
from unittest.mock import patch, Mock
import json
import my_functions
from email.mime.multipart import MIMEMultipart
import pandas as pd

class TestMyFunctions(unittest.TestCase):
    def load_test_config(self, file='test_config.json'):
        # load test configs
        return my_functions.get_config(file)

    # @patch('my_functions.send_email')
    def test_process(self):
        row_data = {'VendorEmails': 'roger.qin@walmart.com',
                    'OwnerEmail': 'roger.qin@walmart.com',
                    'Comments': None,
                    'VENDOR_NAME': 'TEST_VEND',
                    'Chargebacks': 'Late Chargebacks - test use',
                    'month_year': 'Aug-2023'}
        test_config = self.load_test_config()
        my_functions.process(row_data, test_config)

        test_get_data_function = my_functions.get_data(test_config['query']) # Just test get_date() function, not used in test emails.

        '''
        # assert send_email was called
        mock_send_email.assert_called_once()
        # assert send_email was called with MIMEMultipart object as argument
        args, kwargs = mock_send_email.call_args
        self.assertIsInstance(args[0], MIMEMultipart)
        '''

    def test_send_log(self):
        test_log_config = self.load_test_config()['log_config']
        test_df = pd.DataFrame([{
                        "year": 2023,
                        "month": 8,
                        "VENDOR_ID": 100,
                        "VENDOR_NAME": 'TEST_VEND',
                        "owner": 'TEST_OWNER',
                        "OwnerEmail": 'roger.qin@walmart.com',
                        "Chargebacks": 'Test Chargebacks',
                        "Comments": "Success"
                    }])
        my_functions.send_log(test_df, test_log_config)

if __name__ == '__main__':
    unittest.main()
