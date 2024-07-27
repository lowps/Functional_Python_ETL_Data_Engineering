import unittest
from unittest import mock
from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch
from unittest.mock import MagicMock
import os
import sys
import pandas as pd
import psycopg2

"""
Allows Python interpreter to find config and connect modules.
"""
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from config.connect_db import config
from config.connect_db import connect
from config.connect_db import close_connect
from ii_code.iii_push_basedataframe_to_postgres_basetable import push_df_postgres


class TestTopLevelMethods(unittest.TestCase):
    def setUp(self):
        pass

    
    def tearDown(self):
        pass
    

    @unittest.skip('skip')
    @patch('ii_code.iii_push_basedataframe_to_postgres_basetable.logger1')
    @patch('ii_code.iii_push_basedataframe_to_postgres_basetable.close_connect', spec_set = True, autospec = True)
    @patch('ii_code.iii_push_basedataframe_to_postgres_basetable.connect', spec_set = True, autospec = True)
    @patch('ii_code.iii_push_basedataframe_to_postgres_basetable.pd', spec_set = True, autospec = True)
    @patch('ii_code.iii_push_basedataframe_to_postgres_basetable.os.path', spec_set = True, autospec = True)
    def test_push_df_postgres(self, mock_os_path, mock_pd, mock_connect, mock_close_connect, mock_logger1):
        #configure 'abspath()', 'join()', 'read_csv()', 'pandas DataFrame'
        mock_os_path.abspath.return_value = "/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical"
        mock_os_path.join.return_value = "/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/i_data/external/churn_modelling.csv"
        # Set up mock DataFrame
        mock_df = pd.DataFrame({
            'RowNumber': [1, 2],
            'CustomerId': [101, 102],
            'Surname': ['Smith', 'Johnson'],
            'CreditScore': [700, 750],
            'Geography': ['France', 'Spain'],
            'Gender': ['Male', 'Female'],
            'Age': [40, 35],
            'Tenure': [5, 3],
            'Balance': [5000.0, 3000.0],
            'NumOfProducts': [1, 2],
            'HasCrCard': [1, 0],
            'IsActiveMember': [1, 1],
            'EstimatedSalary': [75000.0, 50000.0],
            'Exited': [0, 1]
        })
        mock_pd.read_csv.return_value = mock_df

        #configure 'connect()' return_value, 'mock_connection_object' and 'mock_cursor_object'
        mock_connection_object = MagicMock(return_value = psycopg2.extensions.connection, spec_set = psycopg2.extensions.connection)
        mock_cursor_object = MagicMock(return_value = psycopg2.extensions.cursor, spec_set = psycopg2.extensions.cursor)
        mock_connect.return_value = (mock_cursor_object, mock_connection_object)

        #configure 'execute()', 'fetchone()'
        mock_cursor_object.execute = MagicMock()
        mock_cursor_object.fetchone.return_value = tuple(str(0))

        #call the target function
        push_df_postgres()

        #assertions
        mock_os_path.abspath.assert_called_once()
        mock_os_path.join.assert_called_once()
        mock_pd.read_csv.assert_called_with("/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/i_data/external/churn_modelling.csv")
        mock_cursor_object.fetchone.assert_called()
        mock_cursor_object.execute.assert_called()
        mock_close_connect.assert_called_once_with(mock_cursor_object, mock_connection_object)

        # Check the arguments for the INSERT call
        insert_calls = [call[0][0] for call in mock_cursor_object.execute.call_args_list]
        self.assertTrue(insert_calls[0].startswith("SELECT", 0, 6))
            #asserts SQL 'SELECT' statement utilized
        self.assertTrue(mock_cursor_object.execute.call_count, 2)
            #'.call_count' to verify # of times a callable was called on mock_object

    
    @unittest.skip('skip')
    @patch('ii_code.iii_push_basedataframe_to_postgres_basetable.logger1')
    @patch('ii_code.iii_push_basedataframe_to_postgres_basetable.close_connect', spec_set = True, autospec = True)
    @patch('ii_code.iii_push_basedataframe_to_postgres_basetable.connect', spec_set = True, autospec = True)
    @patch('ii_code.iii_push_basedataframe_to_postgres_basetable.pd', spec_set = True, autospec = True)
    @patch('ii_code.iii_push_basedataframe_to_postgres_basetable.os.path', spec_set = True, autospec = True)
    def test_push_df_postgres_exception_raised(self, mock_os_path, mock_pd, mock_connect, mock_close_connect, mock_logger1):
        #configure 'abspath()', 'join()', 'read_csv()', 'pandas DataFrame'
        mock_os_path.abspath.return_value = "/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical"
        mock_os_path.join.return_value = "/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/i_data/external/churn_modelling.csv"
        mock_df = MagicMock(return_value = pd.DataFrame)
        mock_pd.read_csv.return_value = mock_df

        #configure 'connect()' return_value, 'mock_connection_object' and 'mock_cursor_object'
        mock_connection_object = MagicMock(return_value = psycopg2.extensions.connection, spec_set = psycopg2.extensions.connection)
        mock_cursor_object = MagicMock(return_value = psycopg2.extensions.cursor, spec_set = psycopg2.extensions.cursor)
        mock_connect.return_value = (mock_cursor_object, mock_connection_object)

        #configure exception
        mock_df.iterrows.side_effect = Exception("Error transferring base dataframe data to POSTGRESQL base table")

        #call the target function
        with self.assertRaises(Exception) as e:
            push_df_postgres()

        #assertions
        self.assertIsInstance(e.exception, Exception)
        mock_cursor_object.execute.assert_not_called()
        mock_cursor_object.fetchone.assert_not_called()
        mock_close_connect.assert_called_with(mock_cursor_object, mock_connection_object)

    @unittest.skip('skip')
    @patch('ii_code.iii_push_basedataframe_to_postgres_basetable.push_df_postgres', spec_set = True, autospec = True)   
    def test_main(self, mock_push_df_postgres):
        from ii_code.iii_push_basedataframe_to_postgres_basetable import main

        #execute the main function
        main()

        #assertions
        mock_push_df_postgres.assert_called_once()


if __name__ == '__main__':
    unittest.main()
   
    


