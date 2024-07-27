import os
import sys
import pandas as pd
import psycopg2
import warnings 


"""
Allows Python interpreter to find config and connect modules.
"""
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from config.connect_db import config
from config.connect_db import connect
from config.connect_db import close_connect
from utils.logger import Logger

#create log object from Logger class
'''
Outputs 'directory/python_script.py' name that invoked the log message.
'''
directory_name, file_name = os.path.split(__file__)
directory_name, file_name2 = os.path.split(directory_name)
final_file_name = os.path.join(file_name2,file_name)
logger1 = (final_file_name)


def push_df_postgres() -> None:
    '''
    Purpose:
        create the df and push the df to a postgres table.
    Arg:
        conn- connection to database server.
        cur- connection cursor to the database.
    '''
    fpath: str = os.path.join(os.path.abspath('..'), 'i_data', 'external', 'churn_modelling.csv')
    df: pd.DataFrame = pd.read_csv(fpath)

    #Open cursor and database connection
    cur, conn = connect()
    inserted_row_count: int = 0

    try:
        for _, row in df.iterrows():
            count_query = f"""SELECT COUNT(*) FROM churn_modelling WHERE RowNumber = {row['RowNumber']}"""
            cur.execute(count_query)
            result = cur.fetchone()
                #Querying/Extracting data from postgres database, Methods: fetchall(), fetchone(), fetchmany()   
                        
            if result[0] == 0:
                inserted_row_count += 1
                cur.execute("""INSERT INTO churn_modelling (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
                Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                (int(row[0]), int(row[1]), str(row[2]), int(row[3]), str(row[4]), str(row[5]), int(row[6]), int(row[7]), float(row[8]), int(row[9]), int(row[10]), int(row[11]), float(row[12]), int(row[13])))

        logger1.get_log().info(f'{inserted_row_count} rows from csv file inserted into churn_modelling table successfully.')
    except:
        logger1.get_log().error("Error transferring base dataframe data to POSTGRESQL base table")
        raise
    finally:
        #close cursor and database connection
        close_connect(cur, conn)
        logger1.get_log().info("Database cursor is now close.")
        logger1.get_log().info("Connection to database is now close.")
             

def main():
    push_df_postgres()


if __name__ == "__main__":
    main()





    


    
        
 

     


    


   