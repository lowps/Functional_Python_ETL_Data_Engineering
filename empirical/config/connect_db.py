import psycopg2
import os
import sys
from configparser import ConfigParser


#Configures python interpreter to find built-in modules and enable import statements
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)


#import custom class
from utils.logger import Logger


#returns 'directory_name/file_name.py' in final_file_name
directory_name, file_name = os.path.split(__file__)
_, directory_name = os.path.split(directory_name)
final_file_name = os.path.join(directory_name,file_name)


#create log object from Logger class
#log messages are formatted to contain 'directory/file_name.py' in the configured stdout.
logger1= Logger(final_file_name)


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


def connect():
    '''
    function returns variable 'cur' and 'conn' containing cursor object and connection object
    'psycopg2.extensions.cursor', 'psycopg2.extensions.connection'
    '''
    conn = None
    try:
        #note to self, better to save this in a variable at global scope and pass it as argument because now 'connect()' has dependency with 'config()' function
        params: dict = config() 
            #consider refactoring so that its connect(params)
            #params is a global var outside with, 'params = config()' thus params var holds the output, a return dict. 
                #Helps reduce coupling of connect() to config()
        logger1.get_log().info("Configuration to PostgreSQL successful.")
        conn = psycopg2.connect(**params) #connect to db, returns connect object.
        cur = conn.cursor() #creates cursor, returns cursor object. Allows us to execute SQL syntax via Python code.
        logger1.get_log().info("Connection to database is now open.")
        return cur, conn
    except:
        logger1.get_log().error(f"Error creating database or retrieving associated connection and cursor")
        logger1.get_log().info("Configuration to PostgreSQL unsuccessful.")
        raise Exception("Error in attempting to activate cursor or connection state in respects to Database")
    # finally:
    #     # Closes connection if its open
    #     if conn is not None:
    #         conn.close()
    #         logger1.get_log().info("Connection to database is now close.")
            

def close_connect(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    '''
    Purpose:
    close cursor and connection object to PostgreSQL database

    :param conn: connection object for PostgreSQL database
    :param cur: cursor object for PostgreSQL database
    '''
    conn.commit()
    cur.close()

    if conn is not None:
        conn.close()
        logger1.get_log().info("Connection to database is now close.")


def main() -> None:   
        cur, conn = connect()
        close_connect(cur, conn)
        

if __name__ == "__main__":
    main()