"""
Created on Mar 10, 2017
@author: Souvik
@Program Function: Misclleneous utility functions


"""

import os, shutil


def mkdir(path):

    if not os.path.exists(path):
        os.makedirs(path)

def rmdir(path):

    if os.path.exists(path):
        shutil.rmtree(path)


