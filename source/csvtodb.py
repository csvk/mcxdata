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

    instrument_type = 'FUTCOM'

    def __init__(self, db, type='FUTCOM'):

        # variables

        self.instrument_type = type

        print('Opening Bhavcopy database {}...'.format(db))
        self.conn = sqlite3.connect(db)

    def __del__(self):

        print('Closing DB connection..')
        self.conn.close()

    def test(self):

        c = self.conn.cursor()
        c.execute('''SELECT COUNT(*) FROM tblDump WHERE InstrumentName = "{}"'''.format(self.instrument_type))
        rows = c.fetchall()
        c.close()

        print(rows)

    def unique_symbols(self):

        qry = '''SELECT DISTINCT Symbol FROM tblDump WHERE InstrumentName = "{}"'''.format(self.instrument_type)

        c = self.conn.cursor()
        c.execute(qry)
        rows = c.fetchall()
        c.close()

        return [symbol[0] for symbol in rows]

    def select_symbol_records(self, symbol):

        qry = '''SELECT Symbol, Date, Open, High, Low, Close, VolumeLots, OpenInterestLots, ExpiryDate
                   FROM tblDump 
                  WHERE InstrumentName = "{}"
                    AND Symbol = "{}"'''.format(self.instrument_type, symbol)

        c = self.conn.cursor()
        c.execute(qry)
        rows = c.fetchall()
        c.close()

        print("Printing {} records".format(len(rows)))

        i = 0
        for row in rows:
            i = i + 1
            print(i, row)

    def write_expiries(self):

        truncateQuery = '''DELETE FROM tblExpiries'''

        insertQry = '''INSERT INTO tblExpiries SELECT DISTINCT Symbol, ExpiryDate FROM tblDump 
                  WHERE InstrumentName = "{}"'''.format(self.instrument_type)

        c = self.conn.cursor()
        c.execute(truncateQuery)
        self.conn.commit()
        print('Complete truncate table tblExpiries')
        c.execute(insertQry)
        self.conn.commit()
        print('Complete populate table tblExpiries')
        c.close()

    def expiry_history(self, symbol):

        qry = '''SELECT * 
                   FROM tblExpiries
                  WHERE Symbol = "{}"
                  ORDER BY Symbol ASC, ExpiryDate ASC'''.format(symbol)

        c = self.conn.cursor()
        c.execute(qry)
        rows = c.fetchall()
        c.close()

        return [row[1] for row in rows]

    def symbol_records(self, symbol):

        qry = '''SELECT Symbol, Date, Open, High, Low, Close, VolumeLots, OpenInterestLots, ExpiryDate 
                   FROM tblDump
                  WHERE Symbol = "{}"
                  ORDER BY Symbol ASC, Date ASC, ExpiryDate ASC'''.format(symbol)

        df = pd.read_sql_query(qry, self.conn)

        return df

    def create_continuous_contracts(self, symbols, delta=0):
        '''

        :param symbols: [list of symbols], no need to pass anything if for all symbols
        :return:
        '''

        if len(symbols) == 0:
            symbols = self.unique_symbols()

        for symbol in symbols:
            expiries = self.expiry_history(symbol)

            df = self.symbol_records(symbol)

            dates = list(set(df['Date'].tolist()))
            dates.sort()

            curr_expiry = expiries[0]
            #for date in dates:
            #    sel









