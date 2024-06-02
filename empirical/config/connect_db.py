import psycopg2
import os
import sys
from configparser import ConfigParser
from typing import Any


#Configures python interpreter to find built-in modules and enable import statements
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)


from utils.logger import Logger


#returns 'directory_name/file_name.py' in final_file_name
directory_name: str; file_name: str = os.path.split(__file__)
_:str; directory_name:str = os.path.split(directory_name)
final_file_name: str = os.path.join(directory_name,file_name)


#create log object from Logger class
#log messages are formatted to contain 'directory/file_name.py' in the configured stdout.
logger1= Logger(final_file_name)


#Absolute path of the config file for PostgresSQL database
file_name: str = os.path.abspath(os.path.dirname(__file__))
fpath: str = os.path.join(file_name,'database.ini')


#Docstring describes the breakdown of the 'config' function code.
'''

def config(filename=fpath, section="postgresql"):
    parser=ConfigParser() # create a parser
    parser.read(filename) # read config file
    db = {} # empty dictionary
    if parser.has_section(section): #locates the 'section' of the 'ini' file.
        #'items' goes row by row and fetches the configuration data in the 'ini' file and outputs a tuple set within a list ex:
        #[('host','localhost'), ('database','master'), ('key3','value3')]
        params = parser.items(section) #output in params variable: [('host', 'localhost'), ('database', 'master'), ('user', 'postgres'), ('password', 'erickstory15')]
        for param in params: #param variable holds first item in list: ('host', 'localhost')
            #db is variable holding an '{}' empty dictionary and the key-value pairs will be stored
            #param[0] is slicing, accessing the first element of the list param
            #param[1] is accessing the second element of the list param
            #'=' operator is the assignment operator which assigns the VALUE of the
                #right hand side to the KEY of the left hand side.
            db[param[0]] = param[1] 
    else:
        raise Exception(f'Section {section} is not found in the {filename} file.')
    return db #output is a dictionary: {'host': 'localhost', 'database': 'master', 'user': 'postgres'}

'''

def config(filename: str = fpath, section: str = "postgresql") -> dict:
    parser = ConfigParser()
    parser.read(filename)
    db: dict = {}
    if parser.has_section(section):
        params: list[tuple[str, str]] = parser.items(section) 
        for param in params:             
            db[param[0]]= param[1] 
    else:
        raise Exception(f'Section {section} is not found in the {filename} file.')
    return db 
 


def connect() -> list[psycopg2.extensions.cursor, psycopg2.extensions.connection]:  
    conn = None
    try:
        params: dict = config()
        logger1.get_log().info("Configuration to PostgreSQL successful.")
        conn = psycopg2.connect(**params) #connect to db, returns connect object.
        cur = conn.cursor() #creates cursor, returns cursor object. Allows us to execute SQL syntax via Python code.
        logger1.get_log().info("Connection to database is now open.")
        return cur, conn
    except(Exception, psycopg2.DatabaseError) as e:
        logger1.get_log().error(f"error creating database or retrieving associated connection and cursor: {e}")
        logger1.get_log().info("Configuration to PostgreSQL unsuccessful.")
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
    logger1.get_log().info("Successful commit.")
    cur.close()
    logger1.get_log().info("Database cursor is now close.")
    if conn is not None:
        conn.close()
        logger1.get_log().info("Connection to database is now close.")


def main() -> None:
    cur: psycopg2.extensions.cursor;  conn: psycopg2.extensions.connection = connect()
    close_connect(cur, conn)



if __name__ == "__main__":
    main()
    
    '''
    I was hoping code below will allow me to instantiate my logger object. 
    I thought by declaring name,main idiom the python interpreter will 1st
    process the code from here as "starting point" but for some reason it didnt work
    and my logger1 object wasnt created. Had to create out outside this scope.
    Figure out later why it didn't work as I thought.
    '''
    # directory_name, file_name= os.path.split(__file__)
    # directory_name, file_name2= os.path.split(directory_name)
    # final_file_name= os.path.join(file_name2,file_name)


    #creating object for Logger class
    # logger1= Logger(final_file_name)
    # #Instantiate and 
    # logger1.get_log()
