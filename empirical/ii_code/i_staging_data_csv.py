import os
import sys
import traceback
import logging
from urllib.request  import urlopen
import urllib.request


#Configures python interpreter to find built-in modules and enable import statements
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

#import modules from sibling directories 
from config.connect_db import connect 
from utils.logger import Logger


#returns 'directory_name/file_name.py' in final_file_name
directory_name, file_name = os.path.split(__file__)
_, directory_name = os.path.split(directory_name)
final_file_name = os.path.join(directory_name,file_name)

#create log object from Logger class
#log messages are formatted to contain 'directory/file_name.py' in the configured stdout.
logger1 = Logger(final_file_name)


'''
Define the directory where to ingest raw data.
In future make this portion of code more re-usable with REGEX and parsing instead of 
manually typing in 'churn_modelling.csv' 
'''
dest_folder1: str = os.path.dirname(os.path.dirname(__file__))
dest_folder2: str = os.path.join(dest_folder1, 'i_data', 'external')
dest_path: str = f'{dest_folder2}/churn_modelling.csv'


#url
url: str = "https://raw.githubusercontent.com/lowps/datasets/master/Churn_Modelling.csv"




#Function allows you to download data flat file from url.
def download_data_from_url(url: str, destination_folder: str) -> None:
    """
    Purpose: Downloads a flat file from the specified URL to the specified directory path

    :param url: A url of the flat file it will download
    :param destination_folder: A directory path, that indicates the location the flat file will be downloaded to
    """
    if not os.path.exists(str(dest_folder2)):
        # create folder if it doesnt exist
        os.makedirs(str(dest_folder2))
    try:
        urllib.request.urlretrieve(url, dest_path)
        # print(f'csv file downloaded successfully to {dest_folder2}')
        logger1.get_log().info(f'csv file downloaded successfully to {dest_folder2}')
    except Exception as e:
        logger1.get_log().error(f'Error while downloading the csv file due to: {e}')
        traceback.print_exc()

def main() -> None:
    download_data_from_url(url, dest_folder2)


if __name__ == '__main__':
    # main()
    dest_folder1: str = os.path.dirname(os.path.dirname(__file__))
    dest_folder2: str = os.path.join(dest_folder1, 'i_data', 'external')
    print(dest_folder2)
    