import os
import sys

"""
Allows Python interpreter to find configigure absolute directory to its namespace
"""
PROJECT_ROOT= os.path.dirname(os.path.dirname(__file__))
sys.path.append(PROJECT_ROOT)

from utils.logger import Logger

'''
Purpose: 
Returns 'directory_name/file_name'

Use case:
This is intended to be used for the configuration of a logger object. The return value 
'directory_name/file_name' will be the logger object name and this set-up allows you 
to locate the exact file that raised a log record. 

:param None: N/A
'''

def get_cwd():
    directory_name, file_name= os.path.split(__file__)
    _, directory_name= os.path.split(directory_name)
    return os.path.join(directory_name, file_name)




def main():
    directory_name= get_cwd()
    print(type(directory_name))
    logger1= Logger(directory_name)
    
    mylist= [1,2,3]
    for num in mylist:
        print(num)
    logger1.get_log().info("successfully output data from iterable")




if __name__ == '__main__':
    main()




 