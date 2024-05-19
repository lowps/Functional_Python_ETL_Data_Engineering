import logging
import os
import sys
from logging.handlers import RotatingFileHandler




class Logger:
    '''
    Instructions:
        Add commented syntax to every script that uses logger. 
        Syntax:
         # directory_name, file_name= os.path.split(__file__)
         # directory_name, file_name2= os.path.split(directory_name)
         # final_file_name= os.path.join(file_name2,file_name)
         # logger1= Logger(final_file_name)
    Purpose:
        configures name of logger object to 'directory name/file name'
        now you can pin point the exact file location of the code that 
        output the log record
    '''
    def __init__(self,loggername):
        '''
        A good convention to use when naming loggers is to use a module-level logger, 
        in each module which uses logging, named as follows:
        logger = logging.getLogger(__name__)
        This means that logger names track the package/module hierarchy, 
        and its intuitively obvious where events are logged just from the logger name.
        '''
        #create a logger object
        #getLogger(__name__) function returns a str
        self.logger= logging.getLogger(loggername)
        self.logger.setLevel(logging.DEBUG)

        #create file_handler
        """
        '__file__' outputs absolute path to this script. 
        'os.path.dirname()' outputs relative directory of current working directory, cwd. 
        ex: 1 level above this script: utils.
        """
        log_path= os.path.dirname(os.path.dirname(__file__))
        #try using os.path.join() 
        log_path_output= log_path + '/logs/' + 'out.log'

        '''
        The 'FileHandler' class sends logging output to a disk file.

        class logging.FileHandler(filename, mode='a', encoding=None, delay=False, errors=None)

        A disk file is a computer file that is stored on a magnetic disk. Example of
        disk files include: spreadsheets, database files, text documents, etc.
        '''
        file_handler= logging.FileHandler(log_path_output)
        file_handler.setLevel(logging.DEBUG)

        #create stream_handler
        '''
        The StreamHandler class, located in the core logging package, sends logging output to streams such as 
        sys.stdout, sys.stderr or any file-like object (or, more precisely, any object which supports write() and 
        flush() methods).
        
        class logging.StreamHandler(stream=None) 
            Returns a new instance of the StreamHandler class. If stream is specified, the instance will use it 
            for logging output; otherwise, sys.stderr will be used.
        '''
        stream_handler= logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)

        #create RotatingFileHandler
        """
        1)Allows you to define max amount of storage within log file via 'maxBytes'
        2)Allows you to define max number of logfiles you can generate sequentally after 
        first log file reaches full storage capacity via 'backupCount'
        3)Once you reached specified max number of logfiles, ex:3, it will delete 
        the oldest log file in order to create a new log file. 
        -------------------------------------------------------
        *)Purpose is to free upstorage by deleting old files in order to save new log records.
        When predetermined size is about to be exceeded, the old file is closed and new file is opened for output.
        """
        rotating_file_handler= RotatingFileHandler(filename= log_path_output, mode='w', maxBytes=1024, backupCount=3)
        rotating_file_handler.setLevel(logging.DEBUG)

        #configure Formatter
        formatter= logging.Formatter('%(levelname)s: %(asctime)s from %(name)s: %(process)s: %(funcName)s: %(lineno)s: %(message)s')

        #set formatter to handlers
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)
        # rotating_file_handler.setFormatter(formatter)
            #Don't need formatter if all its doing is rotating files
        
        #add handler to logger object
        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)
        #code below leads to duplicate logs in the output.
        '''
        log message is already being output via '.addHandler(file_handler)'
        No need to create another output via '.addHandler()' adding an additional handler
        which leads to the same output/log-message to the same exact location; think about it
        we already are sending one out to console via 'stream_handler',
        we arleady are sending one out to out.log file via 'file_handler',
        now do we really need a duplicate log message to out.log via 'rotating_file_handler' ???
        Nope. 
        '''
        # self.logger.addHandler(rotating_file_handler)

    # """
    # Define an object for the Logger class then call upon the method
    # 'get_log' to return an instance of logging.getLogger() by calling
    # the attribute 'self.logger' within the constructor. 
    # """
    def get_log(self): #instance method

        """
        Define an object for the Logger class then call upon the method
        'get_log' to return an instance of logging.getLogger() by calling
        the attribute 'self.logger' within the constructor. 
        """
        return self.logger # calling upon 'self.logger' instance 


# def for_fun():
#     # print(Logger.__dict__.keys())
#     # print('-'*100)
#     # print(Logger.__dict__.values())
#     # logger1= Logger('hmk')
#     #dir(logger1)
#     print(help(len))
#     print('-'*100)
#     print(help(round))

    
    # print(dir(__builtins__))

#constructor: __init__(self)
#instance_method(self): used to access instance variables. it can call other 
    #instance, class or static methods within same class. 'self' refers to the current object.
#@classmethod
#class_method(cls): used to access class variable.
#@staticmethod
#static_method(parameter): used for utility code.


if __name__ == '__main__':
    pass




    '''
    The single underscore acts as place holder, it is conventional in python 
    community to use '_' for throwaway variables.
    '''
    # _, file_name= os.path.split(__file__)
    # _, file_name2= os.path.split(_)
    # final_file_name= os.path.join(file_name2,file_name)
    # print(final_file_name)
    
    
    # creating object for the Logger class.
    # logger1= Logger(final_file_name)
    # calling upon 'self.logger' instance 
    # logger1.get_log().info('Log object created successfully')