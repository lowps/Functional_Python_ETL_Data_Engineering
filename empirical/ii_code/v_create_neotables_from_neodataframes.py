import os
import sys
import pandas as pd
import numpy as np
import traceback


PROJECT_ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)


from config.connect_db import connect
from config.connect_db import config
from config.connect_db import close_connect
from utils.logger import Logger

from iv_create_neodataframes_from_basetable import create_neo_base_df
from iv_create_neodataframes_from_basetable import create_creditscore_df
from iv_create_neodataframes_from_basetable import create_exited_age_correlation
from iv_create_neodataframes_from_basetable import create_exited_salary_correlation


'''
Purpose:
    create log object from Logger class.
    Outputs 'directory/python_script.py' name that invoked the log message.
'''
directory_name, file_name= os.path.split(__file__)
directory_name, file_name2= os.path.split(directory_name)
final_file_name= os.path.join(file_name2,file_name)
logger1 = Logger(final_file_name)


def drop_tables():
    cur, conn= connect()
    cur.execute("DROP TABLE IF EXISTS churn_modelling_creditscore")
    cur.execute("DROP TABLE IF EXISTS churn_modelling_exited_age_correlation")
    cur.execute("DROP TABLE IF EXISTS churn_modelling_exited_salaray_correlation")
    
    logger1.get_log().info("churn_modelling table successfully dropped.")
    close_connect(cur, conn)
    logger1.get_log().info("Database cursor and connection object successfully closed.")

def create_new_tables_postgres():
    try:
        cur, conn= connect()
        cur.execute('''CREATE TABLE IF NOT EXISTS churn_modelling_creditscore (geography VARCHAR(50), gender VARCHAR(20), avg_credit_score FLOAT, total_exited INTEGER)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS churn_modelling_exited_age_correlation (geography VARCHAR(50), gender VARCHAR(20), exited INTEGER, avg_age FLOAT, avg_salary FLOAT, number_of_exited_or_not INTEGER)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS churn_modelling_exited_salary_correlation (exited INTEGER, is_greater INTEGER, correlation INTEGER)''')
        logger1.get_log().info("3 tables successfully created in Postgres server.")
    except Exception as error:
        logger1.get_log().error(f"Tables unsuccessfully created due to error message: {error}")

    finally:
        if conn is not None:
            close_connect(cur, conn)
            logger1.get_log().info("Database cursor and connection object successfully closed.")

def insert_creditscore_table(df_creditscore):
    cur, conn= connect()
    query= """INSERT INTO churn_modelling_creditscore (geography, gender, avg_credit_score, total_exited) VALUES (%s,%s, %s,%s)"""
    row_count= 0 
    for _, row in df_creditscore.iterrows():
        values= (row['geography'],row['gender'],row['avg_credit_score'],row['total_exited'])
        cur.execute(query,values)
        row_count += 1
    logger1.get_log().info(f"{row_count} rows inserted into table churn_modelling_creditscore")

    close_connect(cur, conn)
    logger1.get_log().info("Database cursor and connection object successfully closed.")

def insert_exited_age_correlation_table(df_exited_age_correlation):
    cur, conn= connect()
    query= """INSERT INTO churn_modelling_exited_age_correlation (geography, gender, exited, avg_age, avg_salary, number_of_exited_or_not) VALUES (%s,%s,%s,%s,%s,%s)"""
    row_count= 0
    for _, row in df_exited_age_correlation.iterrows():
        values= (row['geography'],row['gender'],row['exited'],row['avg_age'],row['avg_salary'],row['number_of_exited_or_not'])
        cur.execute(query,values)
        row_count += 1
    logger1.get_log().info(f"{row_count} rows inserted into table churn_modelling_exited_age_correlation")

    close_connect(cur, conn)
    logger1.get_log().info("Database cursor and connection object successfully closed.")


def insert_exited_salary_correlation_table(df_exited_salary_correlation):
    cur, conn = connect()
    query = """INSERT INTO churn_modelling_exited_salary_correlation (exited, is_greater, correlation) VALUES (%s,%s,%s)"""
    row_count = 0
    for _, row in df_exited_salary_correlation.iterrows():
        values = (int(row['exited']),int(row['is_greater']),int(row['correlation']))
        cur.execute(query,values)
        row_count += 1
    logger1.get_log().info(f"{row_count} rows inserted into table churn_modelling_exited_salary_correlation")

    close_connect(cur, conn)
    logger1.get_log().info("Database cursor and connection object successfully closed.")


def main():
    '''
     Purpose: 
     i) creates a main dataframe and subsequent dataframes are composed from the main dataframe
     ii) dataframes are transcribe into postgres tables

     :param main_df: creates a main dataframe and subsequent dataframes are composed from main_df
     :param df_creditscore: creates dataframe
     :param df_exited_age_correlation: creates dataframe
     :param df_exited_salary_correlation: creates dataframe
     '''
    main_df = create_neo_base_df()

    df_creditscore = create_creditscore_df(main_df)
    df_exited_age_correlation = create_exited_age_correlation(main_df)
    df_exited_salary_correlation = create_exited_salary_correlation(main_df)

    drop_tables()
    create_new_tables_postgres()
    insert_creditscore_table(df_creditscore)
    insert_exited_age_correlation_table(df_exited_age_correlation)
    insert_exited_salary_correlation_table(df_exited_salary_correlation)


    
if __name__ == '__main__':
    main()
