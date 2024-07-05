import unittest
from unittest import mock
from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch
from unittest.mock import MagicMock
import os
import sys
from configparser import ConfigParser
import psycopg2


#Configures python interpreter to find built-in modules and enable import statements
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from config.connect_db import (file_name,
                               destination_path,
                               config,
                               connect,
                               close_connect)

from utils.logger import Logger 


class TestGlobalVariables(unittest.TestCase):
    
    def setUp(self):
        pass
        #start patching
        '''
        The purpose of patching is to replace external dependencies with mock objects
        and this allows you to focus on testing the behavior of the unit of code itself.

        Under-the-hood, Python is going to call the 'getattr()' to grab the existing 
        target object, external dependency, and replace it with a mock object by calling 'setattr()'
        then it will clean up and set it back to original value/ original state.
        '''

    
    def tearDown(self):
        pass

    @unittest.skip('Skipped for now')
    @patch('config.connect_db.os.path', spec_set = True, autospec = True)
    def test_directoryname_and_filename_global_variables(self, mock_os_path):
        #configure os.path methods 'split()'
        mock_os_path.split.return_value = ('/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config', 'connect_db.py')

        #simulate the behaviour of os.path.split() argument'__file__'
        __file__ = '/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/connect_db.py'

        #test the exact code line
        directory_name, file_name = os.path.split(__file__)
        
        #assertions, assert the expected values 'directory_name' and 'file_name'
        self.assertEqual(directory_name, '/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config')
        self.assertEqual(file_name, 'connect_db.py')

        #assertions, assert mock_os_path.split was called with correct argument
            #verifies that you're not calling the "os.path.split(__file__)" dependency
            #instead, under the hood, within the memory address getattr grabs os.path.split
            #and setattr replaces it with our mock, 'mock_os_path'; we verify this via assert statement below:
        mock_os_path.split.assert_called_once_with(__file__)

    @unittest.skip('Skipped for now')
    @patch('config.connect_db.os.path', spec_set = True, autospec = True)     
    def test_underscore_and_directoryname_global_variables(self, mock_os_path):
        #configure os.path methods 'split()'
        mock_os_path.split.return_value = ('/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical', 'config')

        #simulate the behaviour of os.path.split() argument 'directory_name'
        directory_name = 'config'

        #test the exact code line
        _, directory_name = os.path.split(directory_name)

        #assertions
        self.assertEqual(_, '/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical')
        self.assertEqual(directory_name, 'config')

            #assertions, assert mock_os_path.split was called with correct argument
                #verifies that you're not calling the "os.path.split(directory_name)" dependency
                #instead, under the hood, within the memory address getattr grabs os.path.split
                #and setattr replaces it with our mock, 'mock_os_path'; we verify this via assert statement below:
        mock_os_path.split.assert_called_once_with(directory_name)

    @unittest.skip('skipping for now')
    @patch('config.connect_db.os.path', spec_set = True, autospec = True)
    def test_final_file_name_variable(self, mock_os_path):
        #configure os.path 'join()' method
        mock_os_path.join.return_value('config/connect_db.py')

        #simulate behaviour of os.path.join arguments 'directory_name' and 'file_name'
        directory_name = 'config'
        file_name = 'connect_db.py'

        #test exact target line of code
        os.path.join(directory_name, file_name)

        #assertions
        self.assertEqual(directory_name, 'config')
        self.assertEqual(file_name, 'connect_db.py')
        mock_os_path.join.assert_called_once_with(directory_name, file_name)

    @unittest.skip('skipping for now')
    @patch('config.connect_db.os.path', spec_set = True, autospec = True)
    def test_filename_and_destinationPath_global_variables(self, mock_os_path):
        #configure os.path functions 'dirname()', 'abspath()', 'join() 
        mock_os_path.dirname.return_value = ('/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config')
        mock_os_path.abspath.return_value = ('/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config')
        mock_os_path.join.return_value = ('/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/database.ini')

        #configure arguments 
        __file__ = '/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config'
        file_name = '/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config'

        #test line of code
        os.path.abspath(os.path.dirname(__file__))
        os.path.join(file_name, 'database.ini')

        #assertions
        self.assertEqual(__file__, '/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config')
        self.assertEqual(file_name, '/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config')
       

        #assertions, assert mock_os_path.split was called with correct argument
            #verifies that you're not calling the "os.path.abspath(os.path.dirname(__file__))" and "os.path.join(file_name, 'database.ini')" dependency
            #instead, under the hood, within the memory address getattr grabs os.path.split
            #and setattr replaces it with our mock, 'mock_os_path'; we verify this via assert statement below:
        mock_os_path.dirname.assert_called_once_with(__file__)
        mock_os_path.abspath.assert_called_once_with(os.path.dirname(__file__))
        mock_os_path.join.assert_called_once_with(file_name, 'database.ini')


class Connect_Db_Functions(unittest.TestCase):
    '''
    Define a test case class (sub-class), ConnectDb, that inherits from base class unittest.TestCase. 
    unittest provides a base class, TestCase, which may be used to create new test cases.
    The base class we inherit, TestCase, comes with pre-defined methods such as setUp() and tearDown().
    The setUp() and tearDown() methods allow you to define instructions that will be executed before and after each test method.
    
    TestCase is used to create test cases by subclassing it. Child or subclasses are classes that will inherit from 
    the parent class. That means that each child class will be able to make use of the methods and variables of the 
    parent class.
    
    Inside this class, we define individual test methods, each starting with the prefix test_.
    

    As a special case, patching can be invoked as a class decorator. This is always for subclasses of
    unittest.TestCase, and only methods prefixed with mock.path.TEST_PREFIX are patched ("test_" by default)
    '''

    @unittest.skip('skipping for now')
    @patch('config.connect_db.ConfigParser', spec_set = True, autospec = True)
    def test_config(self, mock_ConfigParser, filename = '/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/database.ini', section = 'postgresql'):
        #configure the behavior of methods 'read', 'has_section', and 'items'
        mock_ConfigParser.read.return_value = ['/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/database.ini']
        mock_ConfigParser.has_section.return_value = True
        mock_ConfigParser.items.return_value = [('host', 'localhost'), ('database', 'master'), ('user', 'postgres'), ('password', 'erickstory15')]

        #configure mock object
        mock_ConfigParser.return_value = mock_ConfigParser

        #simulate the expected arguments 'filename', 'section' for target function "config()"
        filename = '/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/database.ini'
        section = "postgresql"

        #test our target function 
        result = config('/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/database.ini', "postgresql")
        
        #assertions
        '''
        :self.assertEqual: Verifies that the mocks return value matches the target function return value
        :mock_ConfigParser.read.assert_called_once_with(<...>): Verifies that target function "config" interacts correctly w/ "ConfigParser" mock object & processes data as expected based on the mocked behaviour.
        :mock_ConfigParser.has_section.assert_called_once_with(<...>): Verifies that target function "config" interacts correctly w/ "ConfigParser" mock object & processes data as expected based on the mocked behaviour.
        :mock_ConfigParser.items.assert_called_once_with(<...>): Verifies that target function "config" interacts correctly w/ "ConfigParser" mock object & processes data as expected based on the mocked behaviour.
        '''
        expected = {'host': 'localhost', 'database': 'master', 'user': 'postgres', 'password': 'erickstory15'}
        
        self.assertEqual(result, expected)
        self.assertIsInstance(filename, type('/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/database.ini'))
        self.assertIsInstance(section, type('postgresql'))

        mock_ConfigParser.read.assert_called_once_with(filename)
        mock_ConfigParser.has_section.assert_called_once_with(section)
        mock_ConfigParser.items.assert_called_once_with(section)

   
    @unittest.skip('skipping for now')
    @patch('config.connect_db.ConfigParser', spec_set = True, autospec = True)
    def test_config_exception_raised(self, mock_ConfigParser, filename = '/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/database.ini', section = 'non_existent_section'):
        #configure expected values of target class "ConfigParser()" methods 'read', 'has_section'
        mock_ConfigParser.read.return_value = ['/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/database.ini']
        mock_ConfigParser.has_section.side_effect = Exception('Section postgresql is not found in the /Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/database.ini file.')

        #configure mock to raise exception
        mock_ConfigParser.return_value = mock_ConfigParser

        #call target function, test that exception is raised
        with self.assertRaises(Exception) as context:
            config('/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/database.ini', "postgresql")

        #assertions
        self.assertEqual(str(context.exception), 'Section postgresql is not found in the /Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/database.ini file.')
        self.assertRaises(Exception, mock_ConfigParser.has_section)


        mock_ConfigParser.read.assert_called_once_with('/Users/ericklopez/Desktop/Functional_Python_ETL_Data_Engineering/empirical/config/database.ini')
        mock_ConfigParser.has_section.assert_called_once_with('postgresql')
        mock_ConfigParser.items.assert_not_called()


    @unittest.skip('skip')
    @patch('config.connect_db.config', spec_set = True, autospec = True)
    @patch('config.connect_db.psycopg2', spec_set = True, autospec = True)
    def test_connect(self, mock_psycopg2, mock_config):
        #configure 'mock_config' return_value
        mock_config.return_value = {
            'host': 'localhost', 
            'database': 'master', 
            'user': 'postgres', 
            'password': 'erickstory15'
            }
        
        #configure 'mock_psycopg2' functions 'connect()' and 'cursor()' return_value
        mock_psycopg2.return_value.connect.side_effect = psycopg2.extensions.connection
        mock_psycopg2.return_value.connect.cursor.side_effect = psycopg2.extensions.cursor
            #notice this one is a chained functioncall '.connect.cursor'

        mock_psycopg2.return_value = mock_psycopg2
            #saves the child mocks side_effects to the parent mock 'mock_psycopg2'

        #make sure to save it in a tuple. keyword "return" saves results in a tuple
        cur, conn = (psycopg2.extensions.cursor, psycopg2.extensions.connection)

        #execute 'connect()' function and save tuple results 'cur' and 'conn'
        result = connect()
            #'connect()' replaced by 'mock_connect.connect()' via gettr, settr under-the-hood

        #assertions
        params = mock_config()
        self.assertIsInstance(params, dict) 
            #verifies 'mock_config()' return value is dictionary type thus verify verifies expected behaviour
        self.assertIsInstance(result, tuple)
            #verify target 'conect()' return value after utilizing 'mock_connect' is tuple
        mock_psycopg2.connect.assert_called_once()
        mock_psycopg2.connect.assert_called_once_with(
            host = 'localhost', 
            database = 'master', 
            user = 'postgres', 
            password = 'erickstory15'
            )
            #verifies 'mock_psycopg2.connect()' called with correct dict values thus verifies expected behaviour

    
    @unittest.skip('skip')
    @patch('config.connect_db.config', spec_set = True, autospec = True)
    @patch('config.connect_db.psycopg2', spec_set = True, autospec=True)
    def test_connect_exception(self, mock_psycopg2, mock_config):
        #configure 'mock_config' and 'mock_psycopg2' return_values
        mock_config.return_value = {
            'host': 'localhost', 
            'database': 'master', 
            'user': 'postgres', 
            'password': 'erickstory15'
            }

        mock_psycopg2.connect.side_effect = Exception("Error in attempting to activate cursor or connection state in respects to Database")

        mock_psycopg2.return_value = mock_psycopg2

        #execute target function and raise exception
        with self.assertRaises(Exception) as context:
            connect()
        
        #assertions
        self.assertIsInstance(context.exception, Exception)
        self.assertEqual(str(context.exception), "Error in attempting to activate cursor or connection state in respects to Database")
        mock_psycopg2.connect.assert_called_once()
        self.assertRaises(Exception, mock_psycopg2.connect)


    @unittest.skip('skip')
    def mock_conn_side_effect(self):
        mock_conn = MagicMock(spec_set = psycopg2.extensions.connection)
        return mock_conn


    @unittest.skip('skip')
    def mock_cur_side_effect(self):
        mock_cur = MagicMock(spec_set = psycopg2.extensions.cursor)
        return mock_cur


    @unittest.skip('skip')
    @patch('config.connect_db.psycopg2', spec_set = True, autospec = True)
    def test_close_connect(self, mock_psycopg2):
        #configure 
        mock_cursor = MagicMock(spec_set = psycopg2.extensions.cursor)
        mock_connect = MagicMock(spec_set = psycopg2.extensions.connection)
       

        mock_psycopg2.connect.side_effect = mock_connect
        mock_psycopg2.return_value.connect.cursor.side_effect = mock_cursor
            #side_effect is used for raising exceptions
            #side_effect is used for dynamically changing return values
            #side_effect is used for iterables
            #side_effect can be cleared by setting it to None.
            
        

        mock_psycopg2.return_value = mock_psycopg2

        #test target function "close_connect()"
        close_connect(mock_cursor, mock_connect)
        

        #assertions
        mock_connect.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connect.close.assert_called_once()
        self.assertIsInstance(mock_cursor, psycopg2.extensions.cursor)
        self.assertIsInstance(mock_connect, psycopg2.extensions.connection)


if __name__ == "__main__": 
    unittest.main()
    
    