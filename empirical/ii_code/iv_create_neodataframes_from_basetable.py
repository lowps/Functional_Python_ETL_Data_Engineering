import os
import sys
import pandas as pd
import numpy as np


PROJECT_ROOT: str =os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)


from config.connect_db import connect
from config.connect_db import config
from config.connect_db import close_connect
from utils.logger import Logger



'''
Purpose:
    create log object from Logger class.
    Outputs 'directory/python_script.py' name that invoked the log message.
'''
directory_name: str; file_name: str = os.path.split(__file__)
directory_name: str; file_name2: str = os.path.split(directory_name)
final_file_name: str = os.path.join(file_name2,file_name)
logger1= Logger(final_file_name)

'''
Purpose: 
    Query all data from base table.
    Transform the data using pandas.
    This neo_base_df will be the final output.
    3 new tables will be made later using data from new_base_df

functions:
    1) cur.description
        Returns a sequence of column names in order.
    2)
'''


def create_neo_base_df() -> pd.DataFrame:
    '''
    Purpose: n/a

    :param n/a
    '''

    try:
        #weird psycopg2 isnt imported like ii and iii modules.. interesting to see if this will work; run it and observe
        cur, conn = connect()
        logger1.get_log().info("Connection opened.")

        cur.execute('''SELECT * FROM churn_modelling ''')
            #Executes SELECT statement from churn_modelling table using cursor object 'cur'
            #cursor object allows you to run SQL syntax in pythonic code.
        rows = cur.fetchall() 
            #'fetchall()' fetches all rows returned by the previous SELECT statement from code above.
            #outputs a list of tuples w/ all 10_000 rows from churn_modelling table. [(...),(...),(...)]
            #each tuple is equal to 1 row from the churn_modelling table
            #output saved in 'rows' variable

        col_names = [desc[0] for desc in cur.description]
            #This code line creates a list of column names. 
            #Columns is pulled from "cur.description" attribute.
            #"cur.description" outputs a "list of tuples" from the resulting query earlier.
            #Each tuple contains metadata as follows: "(column_name, type, None, None, None, None, null_ok, column_flags)"
            #We loop through each tuple and from each tuple  we pull the column name at position 0 using "desc[0]" 
            #list comprehension, all columns from "churn_modelling" table saved in "col_names" variable.
        df: pd.DataFrame = pd.DataFrame(rows, columns= col_names) 
            #Create a pandas DataFrame with data from SQL table 'churn_modelling'
            #DataFrame shape (10,000,14) via "rows= cur.fetchall() " and "col_names= [desc[0] for desc in cur.description]"

        df.drop('rownumber', axis = 1, inplace = True) 
            #drops the column 'rownumber'
            #DataFrame shape (10,000,13)
        
        index_to_be_null: np.ndarray = np.random.randint(10000, size = 30) 
            #returns 1-d numpy array w/ 30 cells and each w/ a random element between: 
            #0-9999;(start,end,stop). Output is: [8742, 6035, 4592, 8164, 7180, ..., 25 cells later]
            #shape is [30,]; The numpy array is 'standing' in Y direction not X, hence 30 rows and not shape [,30]

        df.loc[index_to_be_null, ['balance','creditscore','geography']] = np.nan 
            #creates new df with 30 rows and 3 columns
            #'loc' is pandas indexing method that allows you to select rows and columns based on labels
            #'index_to_be_null' is an array w/ 30 rows containing random numbers between 0-9999
            #Final output: The 3 columns 'balance','creditscore','geography' and there 
            #rows specified by 'index_to_be_null' is selected, randomly from previous step, and filled w/ NAN value.
        
        most_occured_country = df['geography'].value_counts().index[0] 
            #Returns most frequent row entry, index[0] is row, of geography column with value_counts() function.
             
        df['geography'] = df['geography'].fillna(value=most_occured_country)
            #replaces the NAN values from earlier step, two steps ago, w/ output of previous step.
            #saves it in the Dataframe by overwriting previous column "df['geography']" with
            #the output of "df['geography'].fillna(value=most_occured_country)"
        
        avg_balance = df['balance'].mean()
        df['balance'] = df['balance'].fillna(value=avg_balance)

        median_creditscore = df['creditscore'].median()
        df['creditscore'] = df['creditscore'].fillna(value=median_creditscore)

        return df
    
    except:
        logger1.get_log().error("Error creating tables")
        raise
                  
    
    finally:
        close_connect(cur,conn)
        logger1.get_log().info("Database cursor and connection object successfully closed.")



#def create_creditscore_df(df: pd.DataFrame) ->pd.DataFrame:
#df_creditscore= df.groupby('geography','gender').agg({'creditscore':'mean','exited':'sum'})
#df_creditscore.rename(columns={'exited':'total_exited','creditscore':'avg_credit_score'}, inplace=True)
#df_creditscore.reset_index(inplace=True)
#df_creditscore= df_creditscore.sort_values(by='avg_credit_score', ascending=True)

    

#return df_creditscore
def create_creditscore_df(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Purpose: creates new dataframe from "create_neo_base_df()" output, pandas dataframe.

    :param df: takes pandas dataframe argument.
    '''
    df_creditscore: pd.DataFrame = df[['geography', 'gender', 'exited', 'creditscore']].groupby(['geography','gender']).agg({'creditscore':'mean', 'exited':'sum'})
    df_creditscore.rename(columns={'exited':'total_exited', 'creditscore':'avg_credit_score'}, inplace=True)
    df_creditscore.reset_index(inplace=True)
    df_creditscore.sort_values('avg_credit_score', inplace=True)
    return df_creditscore
    

def create_exited_age_correlation(df: pd.DataFrame) ->pd.DataFrame:
    df_exited_age_correlation: pd.DataFrame = df.groupby(['geography','gender','exited']).agg({
    'age':'mean',
    'estimatedsalary':'mean',
    'exited':'count'}
    ).rename(columns={
        'age':'avg_age',
        'estimatedsalary':'avg_salary',
        'exited':'number_of_exited_or_not'
    }).reset_index().sort_values(by='number_of_exited_or_not', ascending=True)

    df_exited_age_correlation['avg_age'] = round(df_exited_age_correlation['avg_age'],2)
    df_exited_age_correlation['avg_salary'] = round(df_exited_age_correlation['avg_salary'],2)

    return df_exited_age_correlation

'''
Purpose: creates new dataframe from "create_neo_base_df()" output, pandas dataframe.

:param df: takes pandas dataframe argument.
'''
 
def create_exited_salary_correlation(df: pd.DataFrame) ->pd.DataFrame:
    df_salary: pd.DataFrame = df[['geography','gender','exited','estimatedsalary']].groupby(['geography','gender']).agg({'estimatedsalary':'mean'}).sort_values(by='estimatedsalary', ascending=True)
        #new df "df_salary" created
    df_salary.reset_index(inplace=True)

    df_salary['estimatedsalary'] = round(df_salary['estimatedsalary'],0)
        #round row values of "estimatedsalary" column 2 decimal places

    # min_salary= round(df_salary['estimatedsalary'].min(),0)
    # df['is_greater']= df['estimatedsalary'].apply(lambda x: 1 if x > min_salary else 0)
        #creates new dataframe with the name "is_greater"
        #shape (10_000,)
        #note to self:
        #not sure why this code above is even here... its not even used once in below code.

    df_exited_salary_correlation: pd.DataFrame = pd.DataFrame({
    'exited':df['exited'],
    'is_greater':df['estimatedsalary'] > df['estimatedsalary'].min(),
    'correlation':np.where(df['exited'] == (df['estimatedsalary'] > df['estimatedsalary'].min()),1,0)
    })
        #new df "df_exited_salary_correlation" w/ 3 columns: "exited", "isgreater", "correlation" created.
        #note to self
        #look into what that "where" code line is exactly doing, its blurry to me what its exactly doing.
        #"exited" and "correlation" row values are basically identical besides the one false.
        #"isgreater" column row values are all True and 1 False boolean value because applying comparison opperator on "df['estimatedsalary'].min()" 
        #"isgreater" column, ofcourse all will be TRUE besides the minimum which will be equal to it thus FALSE boolean output.
        #such a weird dataframe, df_exited_salary_correlation
    return df_exited_salary_correlation

def main() -> None:
    pass
    # data= create_neo_base_df()
    # create_creditscore_df(data)
    # create_exited_age_correlation(data)


if __name__ == '__main__':
    main()

