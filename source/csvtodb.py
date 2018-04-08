"""
Created on Apr 08, 2018
@author: Souvik
@Program Function: Use sqlite DB instead of csv files


"""

import os
import dates, utils
import pandas as pd
import pickle as pkl
import csv
import sqlite3


class DataDB:
    """ Historical Bhavcopy data"""

    # variables

    def __init__(self, db):

        print('Opening Bhavcopy database {}...', db)
        self.conn = sqlite3.connect(db)

    def test(self):

        c = self.conn.cursor()
        c.execute('''SELECT COUNT(*) FROM tblDump''')
        rows = c.fetchall()

        print(rows)