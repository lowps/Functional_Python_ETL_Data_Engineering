import os
import sys
'''
throwaway file to test code 
'''




'''
Reminder to self:
    1)annotations & Type Hints
    2)look into python library that saves your doc-string and comments w/
        each functions and transfer them over when you call them.
    3)


'''




''''
code I want to experiment with later is marked out
Tasks:
1) figure out how to properly configure 'RotateFileHandler' so it works when
being called on other modules. Only works when you call the log object in the 
file w/ all original code: 'logger.py' module.
2) Experiment w/ Decorators & database connection, configuration, etc.
3) Pipeline restart functionality 
4) Log records w/ cpu usage, memory usage, possibly 'psutils' library.
5)
'''


PROJECT_ROOT=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from utils.logger import Logger
from config.connect_db import config
from config.connect_db import connect
from config.connect_db import close_connect

directory_name, file_name= os.path.split(__file__)
directory_name, file_name2= os.path.split(directory_name)
final_file_name= os.path.join(file_name2,file_name)
logger1= Logger(__name__)
# logger1.get_log().info("hello")
    



# import builtins
# print(dir(builtins))



# def get_icecream(size: str, package: str= 'cup', quantity: int= 1, *flavours: str):
#     '''
#     Purpose: processes multiple customizable ice-cream order.

#     :param size: Size of the ice-cream ['Small', 'Medium', 'Large']
#     :param package: Package in which ice-cream to be received ['cup', 'cone']
#     :param quantity: number of ice-creams to be ordered
#     :param flavours: Multiple Flavours of ice-creams want to order.
#     '''
#     for flavour in flavours:
#         print(f"Packing {flavour} flavored {size} sized, packed in {package} of quanity {quantity}.")
#     print('@@@@@ ALL DONE @@@@@')

# global_var= 'chocolate'


def calculate_interest(p: float, r: float, n: int, /, *args, **kwargs) -> int:
    '''
    #Allowing only positional arguments
    #'/' separates positional arguments from rest of the arguments 

    calculates interest based on keyword based arguments only

    :param p: Amount
    :param r: Rate of interest in %
    :param n: year
    '''
    return(p*r*n)//100

def sendmail(to: str, subject: str, message: str, cc: str='', bcc: str='') -> None:
    '''
    Sends email to any valid email address

    :param to: Person(s) to whom amil is to be sent. 
    :param subject: Subject of the mail visible to recipient before opening mail.
    :param message: message to be sent to recipient.
    :param cc: Recipient to whom carbon copy of the message to be sent.
    :param bcc: Persons who are Behind the Carbon copy of the message.    
    '''
    print('Connecting to mail Server ..')
    print('Sending mail ....')

# def point(x, y, z):
#     print(f"X: {x}, Y: {y}, Z: {z}")

# tuple1= (12, 15, 19)
# list1= [12, 15, 19]

# def multiplyx(x):
#     def withy(y): #closure function
#         return x*y
#     return withy #closure 

# a= 25
# b= 'chocolate'

# print(globals())
# print('==========='*20)
# print(locals())


# # Normal function
# '''
# closer function recalls local variable of enclosed scope, 'function'
# '''
# def greeting():
#     return 'Welcome to Python'
# def uppercase_decorator(function):
#     def wrapper(): #closer function
#         func = function()
#         make_uppercase = func.upper()
#         return make_uppercase
#     return wrapper #closer
# g = uppercase_decorator(greeting)
# print(g())          # output:WELCOME TO PYTHON



# '''This decorator function is a higher order function
# that takes a function as a parameter'''
# def uppercase_decorator(function):
#     def wrapper():
#         func = function()
#         make_uppercase = func.upper()
#         return make_uppercase
#     return wrapper

# @uppercase_decorator
# def greeting():
#     return 'Welcome to Python'
# print(greeting())   # WELCOME TO PYTHON



from functools import wraps


def lowercase_decorator(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        func= function(*args, **kwargs)
        make_lowercase= func.lower()
        return make_lowercase
    return wrapper


@lowercase_decorator
def hello(name: str='') -> str: #'' provides a default value incase no code is provided
    return(f"Hello {name}")




# x= hello.__wrapped__
# print(x('RON JEREMY'))
'''
“__name__” is a special variable managed by Python and it will automatically set 
its value to “__main__” if the script is being run directly and to the name of 
the module, that is, its filename, if it is just being automatically executed as 
part of an import statement.
'''
# print(__name__)
# logger1.get_log().info("successful print")



# g= lowercase_decorator(hello)
# print(g('Ron Jeremy'))
 


def main():
    pass


    #print(help(sendmail))
    # print(calculate_interest(10_000, 10, 1))
    # print('---------'*10)
    # print(multiplyx(45)(3))
    



#    print(callable(point))
#    print(id(point))
#    print(globals())
#    print('@@@'*10)
#    print(dir(point))
     


if __name__ == '__main__':
    main()
    



    


 
    


