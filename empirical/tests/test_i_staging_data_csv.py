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
    
    @unittest.skip("finished")
    @patch("ii_code.i_staging_data_csv.urllib.request", spec_set = True, autospec = True)
    @patch("ii_code.i_staging_data_csv.os.makedirs", spec_set = True, autospec = True)
    @patch("ii_code.i_staging_data_csv.os.path.exists", spec_set = True, autospec = True)
    def test_download_data_from_url_bool_false(self, mock_os_path_exists, mock_os_makedirs, mock_urllib_request):
            #tests the scenario where the directory does not exist
        #target function parameters
        url = "https://raw.githubusercontent.com/lowps/datasets/master/Churn_Modelling.csv"
        destination_folder = "/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/i_data/external"

        #Mock HTTP Message
        mock_http_message = MagicMock()

        #Config MagicMock
        mock_urllib_request.urlretrieve.return_value = (destination_folder, mock_http_message)
            #simulate the tuple return_value
        mock_os_path_exists.return_value = False
            #simulate that the folder does not exist, boolean is False. 

        #Execute the target function you're testing
        download_data_from_url(url, destination_folder)

        #assertions
        mock_urllib_request.urlretrieve.assert_called_once()
        mock_os_path_exists.assert_called_once_with(destination_folder)
        mock_os_makedirs.assert_called_once_with(destination_folder)
        
        #Execute the mock
        result = mock_urllib_request.urlretrieve(url, destination_folder)

        mock_urllib_request.urlretrieve.assert_called_with(url, destination_folder)
        
        #assertions
        self.assertEqual(result, (destination_folder, mock_http_message))
        self.assertEqual(result[0], destination_folder)
        self.assertEqual(result[1], mock_http_message)
        self.assertIsInstance(result, tuple)
        self.assertIsInstance(result[0], str)


    @unittest.skip("finished")
    @patch("ii_code.i_staging_data_csv.urllib.request", spec_set = True, autospec = True)
    @patch("ii_code.i_staging_data_csv.os.makedirs", spec_set = True, autospec = True)
    @patch("ii_code.i_staging_data_csv.os.path.exists", spec_set = True, autospec = True)
    def test_download_data_from_url_bool_true(self, mock_os_path_exists, mock_os_makedirs, mock_urllib_request):
            #tests the scenario where the directory does exist, boolean is True.
        #target function parameters
        url = "https://raw.githubusercontent.com/lowps/datasets/master/Churn_Modelling.csv"
        destination_folder = "/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/i_data/external"
        
        #mock HTTP Message
        mock_http_message = MagicMock()

        #configure mocks
        mock_os_path_exists.return_value = True
        mock_urllib_request.urlretrieve.return_value = (destination_folder, mock_http_message)

        #execute target function you're testing
        result = download_data_from_url(url, destination_folder)

        #assertions
        mock_os_path_exists.assert_called_once_with(destination_folder)
        mock_os_makedirs.assert_not_called()
            #makedirs should not be called since the folder exists
            
        #Execute the mock
        result = mock_urllib_request.urlretrieve(url, destination_folder)

        #assertions
        mock_urllib_request.urlretrieve.assert_called_with(url, destination_folder)
        
        self.assertEqual(result, (destination_folder, mock_http_message))
        self.assertEqual(result[0], destination_folder)
        self.assertEqual(result[1], mock_http_message)
        self.assertIsInstance(result, tuple)
        self.assertIsInstance(result[0], str)


    @unittest.skip("finished")
    @patch("ii_code.i_staging_data_csv.urllib.request", spec_set = True, autospec = True)
    @patch("ii_code.i_staging_data_csv.os.makedirs", spec_set = True, autospec = True)
    @patch("ii_code.i_staging_data_csv.os.path.exists", spec_set = True, autospec = True)
    def test_download_data_from_url_exception_raised(self, mock_os_path_exists, mock_os_makedirs, mock_urllib_request):
    
        #target function parameters
        url = "https://raw.githubusercontent.com/lowps/datasets/master/Churn_Modelling.csv"
        destination_folder = "/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/i_data/external"
        
        #configure mock
        mock_urllib_request.urlretrieve.side_effect = Exception("Error while downloading the csv file")
        mock_os_path_exists.return_value = False

        # #configure mock to return value
        mock_urllib_request.return_value = mock_urllib_request
     
        #Execute target function
        with self.assertRaises(Exception) as context:
            download_data_from_url(url, destination_folder)
        
        #assertions
        self.assertRaises(Exception, mock_urllib_request.urlretrieve)
        self.assertEqual(str(context.exception), "Error while downloading the csv file")
        self.assertIsInstance(context.exception, Exception)

        #assertions
        mock_urllib_request.urlretrieve.assert_called_once()
        mock_os_path_exists.assert_called_once_with(destination_folder)
        mock_os_makedirs.assert_called_once_with(destination_folder)

    
    @unittest.skip("finished")
    @patch('ii_code.i_staging_data_csv.download_data_from_url', spec_set = True, autospec = True)
    def test_main(self, mock_download_data_from_url):
        #import 'main' function after patches to ensure it uses the mock function
        from ii_code.i_staging_data_csv import main

        #configure 'url' and 'dest_folder2' parameters for 'download_data_from_url(url, dest_folder2)'
        url = "https://raw.githubusercontent.com/lowps/datasets/master/Churn_Modelling.csv"
        destination_folder = "/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/i_data/external"


        #execute target code
        result = main()

        #assertions
        mock_download_data_from_url.assert_called_once_with(url, destination_folder)
        self.assertIsNone(result)




if __name__ == '__main__':
    unittest.main()
    #note to self, 
        #left off here, I still need to upload to Github
        #look over your tests (proof) before uploading. 
        #also research if i need a setUp() and tearDown()methods.