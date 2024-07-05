import unittest
from unittest import mock
from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch
from unittest.mock import MagicMock
import urllib.request
from urllib.request  import urlopen
import os
import sys

dir, _ = os.path.split(os.path.dirname(__file__))
sys.path.append(dir)

from ii_code.i_staging_data_csv import download_data_from_url


class Download_Data_From_Url(unittest.TestCase):
    

    @patch("ii_code.i_staging_data_csv.urllib.request", autospec = True)
    @patch("ii_code.i_staging_data_csv.os", autospec = True)
    def test_download_data_from_url(self, mock_os, mock_urllib_request, url = "https://raw.githubusercontent.com/lowps/datasets/master/Churn_Modelling.csv", destination_folder = "/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/i_data/external"):
        print(mock_urllib_request)
        print(mock_os)

        #Config MagicMock
        mock_instance_os_path = MagicMock(spec_set = os.path)
        # mock_instance_urllib_request = MagicMock(spec_set = urllib.request)

        print(mock_instance_os_path)

        #configure os 'makedirs(), os.path 'exists()', urllib.request 'urlretrieve(),




if __name__ == '__main__':
    unittest.main()