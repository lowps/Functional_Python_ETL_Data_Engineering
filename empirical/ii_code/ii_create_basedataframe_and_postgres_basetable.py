import os
import sys
import psycopg2


#Configures python interpreter to find built-in modules and enable import statements
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)


#import modules from sibling directories 
from config.connect_db import connect
from config.connect_db import config
from config.connect_db import close_connect
from utils.logger import Logger



#returns 'directory_name/file_name.py' in final_file_name
directory_name, file_name = os.path.split(__file__)
_, directory_name = os.path.split(directory_name)
final_file_name = os.path.join(directory_name,file_name)

#create log object from Logger class
#log messages are formatted to contain 'directory/file_name.py' in the configured stdout.
logger1= Logger(final_file_name)


"""
Purpose:
    'drop_tables()' function drops 'churn_modelling' table if it exists.
    prevents duplicate tables.
Arg:
    conn- connection to database server.
    cur- connection cursor to the database.
"""
def drop_tables() -> None:
    cur: psycopg2.extensions.cursor;  conn: psycopg2.extensions.connection = connect()
    cur.execute("DROP TABLE IF EXISTS churn_modelling")
    logger1.get_log().info("churn_modelling table successfully dropped.")
    close_connect(cur, conn)
    logger1.get_log().info("Database cursor is now close.")
    logger1.get_log().info("Connection to database is now close.")



#Purpose: create postgreSQL table with desired schema
def create_tables() -> None:
    try:
        cur: psycopg2.extensions.cursor;  conn: psycopg2.extensions.connection = connect()
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling (RowNumber INTEGER PRIMARY KEY, 
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
        logger1.get_log().info(' New Table churn_modelling created successfully to postgres server')
    except:
        logger1.get_log().warning('Unsuccessful creation of tables with specified schema')
    finally:
        close_connect(cur, conn)
        logger1.get_log().info("Database cursor is now close.")
        logger1.get_log().info("Connection to database is now close.")


def main() -> None:
    drop_tables()
    create_tables()

if __name__ == '__main__':
    main()
    

