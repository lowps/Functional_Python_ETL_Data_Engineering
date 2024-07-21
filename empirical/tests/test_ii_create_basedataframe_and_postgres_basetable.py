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

from ii_code.ii_create_basedataframe_and_postgres_basetable import (drop_tables,
                                                                    create_tables)


class TestTopLevelMethods(unittest.TestCase):
    def setUp(self):
        pass

    
    def tearDown(self):
        pass


    @unittest.skip('skip')
    @patch("ii_code.ii_create_basedataframe_and_postgres_basetable.close_connect", spec_set = True, autospec = True)    
    @patch("ii_code.ii_create_basedataframe_and_postgres_basetable.connect", spec_set = True, autospec = True)
    def test_drop_tables(self, mock_connect, mock_close_connect):
        #"Auto-speccing" creates mock objects that have the same attributes and methods as the objects they are replacing,
        # and any functions and methods (including constructors) have the same call signature as the real object.
       
        #"spec or spec-set"- A callable mock created with these two will introspect (examine internally) the call signature
        #when matching calls to the mock. Therefore, it can match the actual calls ARGUMENTS regardless of whether they are passed
        #as positional or keyword.

        #create psycopg2 conneciton and cursor mock_objects to test 'close_connect(cur, conn)' statement called with valid args.
        mock_connection_object = MagicMock(return_value = psycopg2.extensions.connection, spec_set = psycopg2.extensions.connection)
        mock_cursor_object = MagicMock(return_value = psycopg2.extensions.cursor, spec_set = psycopg2.extensions.cursor)

        #configure 'mock_connect' return value
        mock_connect.return_value = (mock_cursor_object, mock_connection_object)

        #call the target function under test
        result = drop_tables()

        #assertions
        mock_connect.assert_called_once()
        mock_cursor_object.execute.assert_called_once_with("DROP TABLE IF EXISTS churn_modelling")
        mock_close_connect.assert_called_once_with(mock_cursor_object, mock_connection_object)
        
        self.assertIsNone(result)


    @unittest.skip('skip')
    @patch("ii_code.ii_create_basedataframe_and_postgres_basetable.close_connect", spec_set = True, autospec = True)    
    @patch("ii_code.ii_create_basedataframe_and_postgres_basetable.connect", spec_set = True, autospec = True)
    def test_drop_tables_exception_raised(self, mock_connect, mock_close_connect):
        #configure Exception 
        mock_connect.side_effect = Exception("Error creating cursor and connection object")

        #execute target function w/ raised exception
        with self.assertRaises(Exception) as context:
            drop_tables()
            #Note, earlier I tried: 
                #if I do "result = drop_tables()" and assert "self.assertIsNone(result)" this will lead to a
                #UnboundLocalError: local variable 'result' referenced before assignment because 
                #'drop_tables()' raises an exception in the CONTEXT MANAGER thus 'result' variable WONT hold 'None' value

            mock_connect.assert_called_once()
                #testing how many times 'connect()' called
            mock_connect.assert_not_called()
                #Note, ran both assertions and both passed
                #Conclusion, assertions not working properly when used within CONTEXT MANAGER
        
        #assertions
        mock_connect.assert_called_once()
            #Note, assertion now working correctly OUTSIDE of CONTEXT MANAGER
            #Verifies that the connect function was called only once.  
        
        self.assertIsInstance(context.exception, Exception)
            #verifies Exception was raised
        self.assertEqual(str(context.exception), "Error creating cursor and connection object")
            #verifies Exception message is correct

        self.assertRaises(Exception, mock_connect)
            #verifies that the 'connect()' method raises an exception thus the 'cursor' and 'connection' object weren't defined.
        
        #'mock_connect.assert_called_once()' THIS FAILED BECAUSE:
            #testing how many times 'connect()' called
            #Note, to self: 
                #"self.assertRaises(Exception, mock_connect)" leads to EVOKING 'connect()' method AGAIN
                #The 'connect()' method was called more times than expected due to the way the 'self.assertRaises' context manager interacts with the mock object. 
                    # Specifically:
                        #'UNEXPECTED CALL COUNT' - The assertion mock_connect.assert_called_once() is intended to verify that connect() is called exactly once. 
                        # However, in this case, connect() was called more than once.

                        #'Effect of self.assertRaises' - 
                            #1) Called Once Here: When using "with self.assertRaises(Exception) as context", the context manager calls the function being tested (drop_tables() in this case).
                            #2) Called a Second Time Here: The statement "self.assertRaises(Exception, mock_connect)"" would cause 'connect()' to be called again, as it directly calls mock_connect to check if the exception is raised.
                                #Note, This is not needed in most cases because mock_connect.side_effect already configures the mock to raise the exception.

        mock_close_connect.assert_called_once_with(None, None)
            # Ensures that 'close_connect()' was called with 'None' values for both 'cur' and 'conn' in 'finally' statement, 
            # which confirms that the exception handling block was executed and the variables were never created.
 

    @unittest.skip('skip')
    @patch("ii_code.ii_create_basedataframe_and_postgres_basetable.close_connect", spec_set = True, autospec = True)    
    @patch("ii_code.ii_create_basedataframe_and_postgres_basetable.connect", spec_set = True, autospec = True)
    def test_create_tables(self, mock_connect, mock_close_connect):
        #configure 'conneciton' and 'cursor' mocks
        mock_connection_object = MagicMock(return_value = psycopg2.extensions.connection, spec_set = psycopg2.extensions.connection)
        mock_cursor_object = MagicMock(return_value = psycopg2.extensions.cursor, spec_set = psycopg2.extensions.cursor)

        #configure return_value of 'mock_connect' and 'connection.cursor()'
        mock_connect.return_value = (mock_cursor_object, mock_connection_object)
        mock_connection_object.cursor.return_value = mock_cursor_object

        #patch 'execute()' method
        mock_cursor_object.execute.return_value = None
            #'None' simulates successful execution

        #execute target function
        result = create_tables()

        #assertions
        self.assertIsNone(result)

        mock_connect.assert_called_once()
        mock_cursor_object.execute.assert_called()
            #asserts the mock.method was called atleast once
        mock_cursor_object.execute.assert_called_once_with("""CREATE TABLE IF NOT EXISTS churn_modelling (RowNumber INTEGER PRIMARY KEY, 
                    CustomerId INTEGER, 
                    Surname VARCHAR(50), 
                    CreditScore INTEGER, 
                    Geography VARCHAR(50), 
                    Gender VARCHAR(20), 
                    Age INTEGER, 
                    Tenure INTEGER, 
                    Balance FLOAT, 
                    NumOfProducts INTEGER, 
                    HasCrCard INTEGER, 
                    IsActiveMember INTEGER, 
                    EstimatedSalary FLOAT, 
                    Exited INTEGER)""")  
        mock_close_connect.assert_called_with(mock_cursor_object, mock_connection_object)
            #assert_called_with(*args, **kwargs) - ensures method called w/ specified args.


    @unittest.skip('skip')
    @patch("ii_code.ii_create_basedataframe_and_postgres_basetable.close_connect", spec_set = True, autospec = True)    
    @patch("ii_code.ii_create_basedataframe_and_postgres_basetable.connect", spec_set = True, autospec = True)
    def test_create_tables_exception_raised(self, mock_connect, mock_close_connect):
        #configure 'conneciton', 'cursor' mocks and 'mock_connect' return_value
        mock_connection_object = MagicMock(return_value = psycopg2.extensions.connection, spec_set = psycopg2.extensions.connection)
        mock_cursor_object = MagicMock(return_value = psycopg2.extensions.cursor, spec_set = psycopg2.extensions.cursor)
        mock_connect.return_value = (mock_cursor_object, mock_connection_object)

        #configure '.execute()' callable raises an Exception
        mock_cursor_object.execute.side_effect = Exception("Unsuccessful creation of tables with specified schema")

        #execute target function w/ raised exception
        with self.assertRaises(Exception) as context:
            create_tables()
        
        #assertions
        self.assertIsInstance(context.exception, Exception)
            #verifies the SPECIFIED exception raised, if a different exception was raised instead of the specified one 
            #then it will fail the unittest
        self.assertEqual(str(context.exception), "Unsuccessful creation of tables with specified schema")
            #verifies Exception message is correct
       
        # self.assertRaises(Exception, mock_cursor_object.execute)
            #verifies that the '.execute' method raised an exception
            #Note, I commented out this assertion because in the grand scheme of things 
            #'self.assertEqual(str(context.exception), "Unsuccessful creation of tables with specified schema")'
            #and 'self.assertRaises(Exception, mock_cursor_object.execute)' is REDUNDANT/BOILERPLATE CODE
            #because 'mock_cursor_object.execute' is what produces that error message "Unsuccessful creation of tables with specified schema"
        
        mock_close_connect.assert_called_with(mock_cursor_object, mock_connection_object)
            #assert_called_with(*args, **kwargs) - ensures method called w/ specified args.


    @unittest.skip('skip')
    @patch("ii_code.ii_create_basedataframe_and_postgres_basetable.create_tables", spec_set = True, autospec = True)
    @patch("ii_code.ii_create_basedataframe_and_postgres_basetable.drop_tables", spec_set = True, autospec = True)
    def test_main(self, mock_drop_tables, mock_create_tables):
        #import main function after patches to ensure it uses the mock function
        from ii_code.ii_create_basedataframe_and_postgres_basetable import main
        
        #execute the main function
        main()

        #assertions
            #verify that drop_tables and create_tables were called once
        mock_drop_tables.assert_called_once()
        mock_create_tables.assert_called_once()
        
        #note to self:
            #Unit testing the 'main()' function can be useful to ensure that the overall workflow of your script is correctly executed
            #testing 'main()' ensures that these functions are called in the correct order and that the overall process works as intended.
                #1)Integration Testing: It helps ensure that the individual components (functions) work together as expected.
                #2)Flow Verification: It verifies the order of function calls.
                #3)Error Handling: It ensures that errors are handled properly at the top level.             

if __name__ == '__main__':
    unittest.main()
  