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

    def dump_record_count(self):

        c = self.conn.cursor()
        c.execute('''SELECT COUNT(*) FROM tblDump WHERE InstrumentName = "{}"'''.format(self.instrument_type))
        rows = c.fetchall()
        c.close()

        print("Total number of records in the data dump: {}".format(rows[0][0]))

    def unique_symbols(self):

        qry = '''SELECT DISTINCT Symbol FROM tblDump WHERE InstrumentName = "{}"'''.format(self.instrument_type)

        c = self.conn.cursor()
        c.execute(qry)
        rows = c.fetchall()
        c.close()

        return [symbol[0] for symbol in rows]

    def trading_days(self):
        '''
        Populate trading_days dictionary
        :param symbols: [list of symbols], no need to pass anything if for all symbols
        :return: dict {trading_day#1: idx#1, ....}
        '''

        qry = '''SELECT DISTINCT Date FROM tblDump 
                  WHERE InstrumentName = "{}" ORDER BY Date'''.format(self.instrument_type)

        c = self.conn.cursor()
        c.execute(qry)
        rows = c.fetchall()
        c.close()

        dates = [row[0] for row in rows]
        date_idx = [i + 1 for i in range(0, len(rows))]

        return dict(zip(dates, date_idx))

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

        insertQry = '''INSERT INTO tblExpiries
                       SELECT DISTINCT Symbol, ExpiryDate FROM tblDump
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
                    AND InstrumentName = "{}"
                  ORDER BY Symbol ASC, Date ASC, ExpiryDate ASC'''.format(symbol, self.instrument_type)

        df = pd.read_sql_query(qry, self.conn)

        return df

    def create_continuous_contracts(self, symbols, delta=0):
        '''
        Create continuous contracts with rollover day on delta trading days from expiry
        delta = 0 means rollover happens on expiry day
        :param symbols: [list of symbols], no need to pass anything if for all symbols
        :return:
        '''

        if len(symbols) == 0:
            symbols = self.unique_symbols()

        trading_days = self.trading_days()

        for symbol in symbols:
            expiries = self.expiry_history(symbol)

            df = self.symbol_records(symbol)

            dates = list(set(df['Date'].tolist()))
            dates.sort()

            curr_expiry = expiries[0]
            #for date in dates:
            #    sel
            #print("SVK")
            #print(df)
            #sel_record = df.loc[df['ExpiryDate'] == curr_expiry]
            sel_record = df[df['ExpiryDate'] == curr_expiry]
            print(sel_record)









