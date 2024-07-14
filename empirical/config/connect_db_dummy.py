import psycopg2
import os
import sys
from configparser import ConfigParser
import logging
from functools import wraps

#Configures python interpreter to find built-in modules and enable import statements
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)


# #import custom class
# from utils.logger import Logger


#returns 'directory_name/file_name.py' in final_file_name
directory_name, file_name = os.path.split(__file__)
_, directory_name = os.path.split(directory_name)
final_file_name = os.path.join(directory_name,file_name)


# #create log object from Logger class
# #log messages are formatted to contain 'directory/file_name.py' in the configured stdout.
# logger1= Logger(final_file_name)


#Absolute path of the config file for PostgresSQL database
file_name: str = os.path.abspath(os.path.dirname(__file__))
destination_path: str = os.path.join(file_name,'database.ini')

def config(filename: str = destination_path, section: str = "postgresql") -> dict:
    parser = ConfigParser()
    parser.read(filename)
    db: dict = {}
    if parser.has_section(section):
        params: list[tuple[str, str]] = parser.items(section)
        for param in params:             
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} is not found in the {filename} file.')
    return db 


class NotFoundError(Exception): #subclasses of python main super class Exception
    pass

class NotAuthorizedError(Exception): #subclasses of python main super class Exception
    pass

def retrieve_db_credentials(file_path = destination_path, section = 'postgresql'):
    parser = ConfigParser()
    parser.read(file_path)
    db: dict = {}
    if parser.has_section(section):
        params: list[tuple[str, str]] = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception
        return []
    # print(db)
    return db


class PSYCOPG2:
    def __init__(self, file_path = 'destination_path', **db_credentials):
        self.file_path = file_path
        self.db_credentials = db_credentials


    def __enter__(self):
        '''Context Manager Usage: The with statement calls the __enter__ method 
        when entering the block and the __exit__ method when exiting the block, 
        even if an exception occurs. Reduces boilerplate code.'''
        self.conn = psycopg2.connect(**self.db_credentials)
        print('active connection')
        return self.conn.cursor()
    

    def __exit__(self, type, value, traceback):
        '''Context Manager Usage: The with statement calls the __enter__ method 
        when entering the block and the __exit__ method when exiting the block, 
        even if an exception occurs. Reduces boilerplate code.'''
        self.conn.close()
        print('closed connection')

#get the database credentials
db_credentials = retrieve_db_credentials()

# with PSYCOPG2(**db_credentials) as cursor:
#     # Execute database operations using the cursor
#     cursor.execute("SELECT * FROM some_table")
#     rows = cursor.fetchall()
#     for row in rows:
#         print(row)


# def fetch_data():
#     #context manager; creation and destruction of resources in a single place
#     with PSYCOPG2(destination_path) as cur:
#         #execute query
#         cur.execute('Select * From <..> where <..>')

#         #fetch the result and turn it into dict, list, etc
#         result = None

#         return result


# def connect(**kwargs):
#     '''{'host': 'host_name', 'database': 'database_name', 'user': 'user_name', 'password': 'password'}'''
#     conn = None
#     try:
#         params = kwargs
#         conn = psycopg2.connect(**params)
#         cur = conn.cursor()
#         print('connection and cursor object created')
#         return cur, conn
#     except psycopg2.OperationalError as e:
#         print(e)
#         raise NotFoundError('unable to find database credentials: database name, user name, password, host, port')
#     finally:
#         if conn is not None:
#             conn.close()
#             print('connection closed')


# def create_logger():
#     #create a logger object
#     logger = logging.getLogger('exc_logger')
#     logger.setLevel(logging.INFO)   

#     #create a file to store all the logged exceptions
#     logfile = logging.FileHandler('exc_logger.log')

#     fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     formatter = logging.Formatter(fmt)

#     logfile.setFormatter(formatter)
#     logger.addHnadler(logfile)

#     return logger

# logger = create_logger()

# def exception(logger):
#     def decorator(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             try:
#                 return func(*args, **kwargs)
#             except:
#                 issue = "exception in "+func.__name__+"\n"
#                 issue = issue+"==================\n"
#                 logger.exception(issue)
#                 raise
#         return wrapper
#     return decorator
            

# @exception(logger)
# def divideByZero():
#     return 12/0

def main():
    x = retrieve_db_credentials()
    obj = PSYCOPG2(destination_path, **x)


if __name__ == '__main__':
    # print("\n")
    # divideByZero()
    main()


