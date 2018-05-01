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
from sqlalchemy import create_engine


class DataDB:
    """ Historical Bhavcopy data"""

    # variables

    instrument_type = 'FUTCOM'
    trading_days = dict()

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

    def __init__(self, db, type='FUTCOM'):

        # variables

        self.instrument_type = type    

        print('Opening Bhavcopy database {}...'.format(db))
        self.conn = sqlite3.connect(db)
        self.engine = create_engine('sqlite:///{}'.format(db))

        self.trading_days = self.trading_days()

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

    def trading_day(self, date):
        '''
        Return trading day idx from trading_days
        :param symbols: date in YYYY-MM-DD format
        :return: trading day idx from trading_days
        '''

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

    def insert_records(self, df):

        df.to_sql('tblFutures', self.engine, index=False, if_exists='replace')

    def create_continuous_contracts(self, symbols, delta=0):
        '''
        Create continuous contracts with rollover day on delta trading days from expiry
        delta = 0 means rollover happens on expiry day
        :param symbols: [list of symbols], no need to pass anything if for all symbols
        :return:
        '''

        if len(symbols) == 0: # no symbol passed, default to all symbols
            symbols = self.unique_symbols()

        #trading_days = self.trading_days() # get all trading_days

        for symbol in symbols:
            #expiries = self.expiry_history(symbol)
            expiries = ['2011-03-31', '2011-04-28', '2011-05-31']

            df = self.symbol_records(symbol)

            dates = list(set(df['Date'].tolist())) # unique dates
            dates.sort()

            df['TradingDay'] = [self.trading_days[date] for date in df['Date']]    

            records = pd.DataFrame()

            next_trading_day = 0
            expiry_idx = 0

            for expiry in expiries:
                curr_expiry = expiries[expiry_idx]
                curr_expiry_idx = self.trading_days[curr_expiry]
                sel_records = df.loc[(df['ExpiryDate'] == curr_expiry) & 
                                     (df['TradingDay'] < curr_expiry_idx + delta ) &
                                     (df['TradingDay'] >= next_trading_day)]
                records = pd.concat([records, sel_records], axis=0)

                next_trading_day = curr_expiry_idx + delta
                expiry_idx = expiry_idx + 1

            #print(records)

            display = records.reindex_axis(['Date', 'ExpiryDate', 'TradingDay'], axis=1)
            print(display)
            df_insert = records.drop(['TradingDay'], axis=1)

            self.insert_records(df_insert)










