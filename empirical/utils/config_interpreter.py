import os
import sys



'''
Configures python interpreter to find built-in modules and enable import statements

:param project_dir: variable holds the absolute path of current working directory (cwd)
'''
def config_python_interpreter():
    project_dir= os.path.dirname(os.path.dirname(__file__)) 
    sys.path.append(project_dir)

