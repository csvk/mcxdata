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
    trading_day_idx = dict()
    trading_day_idx_rev = dict()

    def set_trading_day_idx(self):
        '''
        Populate trading_day_idx, trading_day_idx_rev dictionary
        '''

        qry = '''SELECT DISTINCT Date FROM tblDump 
                  WHERE InstrumentName = "{}" ORDER BY Date'''.format(self.instrument_type)

        c = self.conn.cursor()
        c.execute(qry)
        rows = c.fetchall()
        c.close()

        dates = [row[0] for row in rows]
        date_idx = [i + 1 for i in range(0, len(rows))]

        trading_day_idx, trading_day_idx_rev = dict(zip(dates, date_idx)), dict(zip(date_idx, dates))

    def __init__(self, db, type='FUTCOM'):

        # variables

        self.instrument_type = type    

        print('Opening Bhavcopy database {}...'.format(db))
        self.conn = sqlite3.connect(db)
        self.engine = create_engine('sqlite:///{}'.format(db))

        #self.trading_day_idx = self.trading_days()
        self.set_trading_day_idx()

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
        Return trading day idx from trading_day_idx
        :param symbols: expiry date in YYYY-MM-DD format
        :return: trading day idx from trading_day_idx
        '''

        trading_day_list = list(self.trading_day_idx.keys())
        trading_day_list.sort()

        last_trading_day = trading_day_list[len(trading_day_list) - 1]
        if date > last_trading_day: # If passed expiry date is beyond last available bar
            weekdays_till_date = dates.dates(last_trading_day, date, 
                                             ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'])
            return self.trading_day_idx[last_trading_day] + len(weekdays_till_date) - 1
        else:
            if date in self.trading_day_idx: # If passed expiry date is a trading day
                return self.trading_day_idx[date]
            else: # If passed expiry date is not a trading day, use previous trading day
                found = False
                save_date = date
                while not found:
                    date = dates.relativedate(date, days=-1)
                    if date in self.trading_day_idx:
                        return self.trading_day_idx[date]
                        found = True
                    if dates.datediff(save_date, date) > 10:
                        return None

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
        '''
        Write all expiry dates in tblExpiries
        '''

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
        '''
        Return list of expiry dates for symbol
        :param symbols: symbol
        :return: [expiry_date1, expiry_date2,...]
        '''

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
        '''
        Insert passed records into tblFutures
        '''

        df.to_sql('tblFutures', self.engine, index=False, if_exists='append')

    def create_continuous_contracts(self, symbols=[], delta=0):
        '''
        Create continuous contracts with rollover day on delta trading days from expiry
        delta = 0 means rollover happens on expiry day
        :param symbols: [list of symbols], no need to pass anything if for all symbols
        :return:
        '''

        if len(symbols) == 0: # no symbol passed, default to all symbols
            symbols = self.unique_symbols()

        records = pd.DataFrame()
        for symbol in symbols:
            print("Creating for {}".format(symbol))
            expiries = self.expiry_history(symbol)
            #expiries = ['2011-03-31', '2011-04-28', '2011-05-31']

            df = self.symbol_records(symbol)

            #dates = list(set(df['Date'].tolist())) # unique dates
            dates = df['Date'].unique()
            dates.sort()

            #print(dates)

            df['TradingDay'] = [self.trading_day_idx[date] for date in df['Date']]    

            next_trading_day_idx = 0
            expiry_idx = 0

            for expiry in expiries:
                curr_expiry = expiries[expiry_idx]
                curr_expiry_idx = self.trading_day(curr_expiry)
                sel_records = df.loc[(df['ExpiryDate'] == curr_expiry) & 
                                     (df['TradingDay'] < curr_expiry_idx - delta ) &
                                     (df['TradingDay'] >= next_trading_day_idx)]
                records = pd.concat([records, sel_records], axis=0)

                next_trading_day_idx = curr_expiry_idx - delta
                expiry_idx = expiry_idx + 1
                #print(curr_expiry, curr_expiry_idx, next_trading_day_idx)

        #print(records)

        display = records.reindex(['Date', 'ExpiryDate', 'TradingDay'], axis=1)
        #print(display)
        df_insert = records.drop(['TradingDay'], axis=1)

        self.insert_records(df_insert)

        #self.manage_missed_records(symbols, delta)

    def manage_missed_records(self, symbols=[], delta=0):
        '''
        Identify records missed while creating continuous contracts and insert them
        :param symbols: [list of symbols], no need to pass anything if for all symbols
        :return:
        '''

        if len(symbols) == 0: # no symbol passed, default to all symbols
            symbols = self.unique_symbols()

        # Identify symbol-date combinations which were available in tblDump but not included in tblFutures

        qry = '''SELECT tblDump.Symbol, tblDump.Date, tblDump.ExpiryDate, tblDump.VolumeLots
                   FROM tblDump LEFT OUTER JOIN tblFutures
                     ON tblDump.Symbol = tblFutures.Symbol
                    AND tblDump.Date = tblFutures.Date
                  WHERE tblFutures.date is NULL
                  ORDER BY tblDump.Symbol ASC, tblDump.Date ASC, tblDump.ExpiryDate ASC'''

        missed_records = pd.read_sql_query(qry, self.conn)
        #select_missed_records = missed_records[missed_records.Symbol.isin(symbols)]

        select_missed_records = missed_records if len(symbols) == 0 else missed_records[missed_records.Symbol.isin(symbols)]

        symbols_considered = dict()

        selected_records, eligible_records = pd.DataFrame(), pd.DataFrame()

        curr_symbol = ""

        for row in select_missed_records.itertuples(index=True, name='Pandas'):
            symbol, date, expiry_date = getattr(row, "Symbol"), getattr(row, "Date"), getattr(row, "ExpiryDate")
            #print(symbol, date, expiry_date)

            if symbol != curr_symbol:
                print(symbol, ' processing...')
                curr_symbol = symbol

            if symbol not in symbols_considered:
                symbols_considered[symbol] = '1900-01-01'
            
            if date <= symbols_considered[symbol]:
                continue

            prev_exp_qry = '''SELECT ExpiryDate FROM tblFutures 
                               WHERE Symbol = "{}" AND Date < "{}" ORDER BY Date DESC'''.format(symbol, date)
            next_exp_qry = '''SELECT ExpiryDate FROM tblFutures 
                               WHERE Symbol = "{}" AND Date > "{}" ORDER BY Date ASC'''.format(symbol, date)                          

            c = self.conn.cursor()
            c.execute(prev_exp_qry)
            prev_exp = c.fetchone()
            if prev_exp is None:
                prev_exp = expiry_date
                print('Prev expiry not found, setting to current expiry {}'.format(expiry_date))
            else:
                prev_exp = prev_exp[0]
            c.execute(next_exp_qry)
            next_exp = c.fetchone()
            if next_exp is None:
                next_exp = expiry_date
                print('Next expiry not found, setting to current expiry {}'.format(expiry_date))
            else:
                next_exp = next_exp[0]
            #print(symbol, date, expiry_date, prev, next)
            #c.close()

            selected_expiry_row_qry = '''SELECT COUNT(*), SUM(VolumeLots) FROM tblFutures
                                          WHERE Symbol = "{}" 
                                            AND Date >= "{}" 
                                            AND ExpiryDate = "{}"'''.format(symbol, date, prev_exp)

            
            #print(selected_expiry_row_qry)
            c.execute(selected_expiry_row_qry)
            result = c.fetchone()

            # temp to be removed

            selected_expiry_row_qry_temp = '''SELECT * FROM tblFutures
                                          WHERE Symbol = "{}" 
                                            AND Date >= "{}" 
                                            AND ExpiryDate = "{}"'''.format(symbol, date, prev_exp)

            selected_records = pd.concat([selected_records, pd.read_sql_query(qry, self.conn)], axis=0)

            # end temp

            eligible_missed_records = select_missed_records[(missed_records.Symbol == symbol) &
                                                            (missed_records.ExpiryDate == next_exp) &
                                                            (missed_records.Date >= date)]

            #eligible_missed_records = eligible_missed_records[missed_records.Date >= date]
                                                        
            eligible_missed_records_count = eligible_missed_records.shape[0]

            #print(eligible_missed_records)

            eligible_records = pd.concat([eligible_records, eligible_missed_records], axis=0)

            #print(symbol, date, 'exp ', expiry_date, 'selected count, volume ', result, 
            #      'eligible count, volume ', eligible_missed_records_count, eligible_missed_records.VolumeLots.sum())

            #print(eligible_missed_records.iloc[eligible_missed_records.shape[0] - 1])

            if eligible_missed_records.shape[0] > 0:
                symbols_considered[symbol] = eligible_missed_records.iloc[eligible_missed_records.shape[0] - 1]['Date']

            c.close()
        
        selected_records.to_csv('selected_records.csv', sep=',', index=False)
        eligible_records.to_csv('eligible_records.csv', sep=',', index=False)









