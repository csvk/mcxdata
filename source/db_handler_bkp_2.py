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
#from pympler.tracker import SummaryTracker, ObjectTracker
import gc


class DataDB:
    """ Historical Bhavcopy data"""

    # variables

    instrument_type = 'FUTCOM'
    trading_day_idx = dict()
    trading_day_idx_rev = dict()
    selected_records_file = 'selected_records.csv'
    eligible_records_file = 'eligible_records.csv'
    duplicate_records_file = 'duplicate_records.csv'
    duplicate_ignored_file = 'duplicate_ignored.csv'

    def set_trading_day_idx(self):
        """
        Populate trading_day_idx, trading_day_idx_rev dictionary
        """

        qry = '''SELECT DISTINCT Date FROM tblDump 
                  WHERE InstrumentName = "{}" ORDER BY Date'''.format(self.instrument_type)

        c = self.conn.cursor()
        c.execute(qry)
        rows = c.fetchall()
        c.close()

        dates = [row[0] for row in rows]
        date_idx = [i + 1 for i in range(0, len(rows))]

        self.trading_day_idx, self.trading_day_idx_rev = dict(zip(dates, date_idx)), dict(zip(date_idx, dates))

    def __init__(self, db, type='FUTCOM'):

        # variables

        self.instrument_type = type

        print('Opening Bhavcopy database {}...'.format(db))
        self.conn = sqlite3.connect(db)
        self.engine = create_engine('sqlite:///{}'.format(db))

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
        """
        Return trading day idx from trading_day_idx
        :param symbols: expiry date in YYYY-MM-DD format
        :return: trading day idx from trading_day_idx
        """

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

    def write_expiries(self):
        """
        Write all expiry dates in tblExpiries
        """

        truncate_query = '''DELETE FROM tblExpiries'''

        insert_query = '''INSERT INTO tblExpiries
                       SELECT DISTINCT Symbol, ExpiryDate FROM tblDump
                        WHERE InstrumentName = "{}"'''.format(self.instrument_type)

        c = self.conn.cursor()
        c.execute(truncate_query)
        self.conn.commit()
        print('Complete truncate table tblExpiries')
        c.execute(insert_query)
        self.conn.commit()
        print('Complete populate table tblExpiries')
        c.close()

    def expiry_history(self, symbol):
        """
        Return list of expiry dates for symbol
        :param symbols: symbol
        :return: [expiry_date1, expiry_date2,...]
        """

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
        """
        Insert passed records into tblFutures
        """

        df.to_sql('tblFutures', self.engine, index=False, if_exists='append')

    def create_continuous_contracts(self, symbols=[], delta=0):
        """
        Create continuous contracts with rollover day on delta trading days from expiry
        delta = 0 means rollover happens on expiry day
        :param symbols: [list of symbols], no need to pass anything if for all symbols
        :return:
        """

        if len(symbols) == 0: # no symbol passed, default to all symbols
            symbols = self.unique_symbols()

        records = pd.DataFrame()
        for symbol in symbols:
            print("Creating for {}".format(symbol))
            expiries = self.expiry_history(symbol)

            df = self.symbol_records(symbol)

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

        df_insert = records.drop(['TradingDay'], axis=1)

        df_unique = df_insert.drop_duplicates(['Symbol', 'Date'], keep=False)
        df_duplicate = df_insert[df_insert.duplicated(['Symbol', 'Date'], keep=False)]

        try:
            os.remove(self.duplicate_records_file)
        except OSError:
            pass
        if len(df_duplicate.index) > 0:
            df_duplicate.to_csv(self.duplicate_records_file, sep=',', index=False)

        self.insert_records(df_unique)

    def update_continuous_contract(self, symbols=[]):

        print('start update continuous contract')

        selected_records = pd.read_csv(self.selected_records_file)
        eligible_records = pd.read_csv(self.eligible_records_file)

        c = self.conn.cursor()

        # delete records
        prev_symbol = ""
        for idx, row in selected_records.iterrows():
            if row['Symbol'] not in symbols and symbols != []:
                continue
            if row['Symbol'] != prev_symbol:
                print('deleting', row['Symbol'])
            delete_qry = '''DELETE FROM tblFutures 
                             WHERE Symbol = "{}"
                               AND Date = "{}"'''.format(row['Symbol'], row['Date'])
            c.execute(delete_qry)
            prev_symbol = row['Symbol']
        self.conn.commit()

        duplicate_ignored = pd.DataFrame()
        for idx, row in eligible_records.iterrows():
            if row['Symbol'] not in symbols and symbols != []:
                continue

            insert_row = (row['Symbol'], row['Date'], row['Open'], row['High'], row['Low'], row['Close'],
                          row['VolumeLots'], row['OpenInterestLots'], row['ExpiryDate'])

            check_qry = '''SELECT * FROM tblFutures WHERE Symbol = "{}" AND Date = "{}"'''.format(row['Symbol'],
                                                                                              row['Date'])

            duplicate_record = pd.read_sql_query(check_qry, self.conn)

            if duplicate_record.empty:
                c.execute('''INSERT INTO tblFutures VALUES (?,?,?,?,?,?,?,?,?)''', insert_row)
            else:
                duplicate_record['Flag'] = "Dup"
                row['Flag'] = "Ign"
                row_frame = row.to_frame().T

                duplicate_ignored = pd.concat([duplicate_ignored, duplicate_record, row_frame], axis=0)

        try:
            os.remove(self.duplicate_ignored_file)
        except OSError:
            pass
        duplicate_ignored.to_csv(self.duplicate_ignored_file, sep=',', index=False)

        self.conn.commit()
        c.close()

    def prev_and_next_dates(self, c, symbol, expiry_date, symbols_latest_date, symbols_latest_exp, start, end):

        prev_date_qry = '''SELECT Date, ExpiryDate FROM tblFutures 
                                           WHERE Symbol = "{}" AND Date < "{}" ORDER BY Date DESC'''.format(symbol,
                                                                                                            start)
        next_date_qry = '''SELECT Date, ExpiryDate FROM tblFutures 
                                           WHERE Symbol = "{}" AND Date > "{}" ORDER BY Date ASC'''.format(
            symbol, end)

        c.execute(prev_date_qry)
        prev_date_record = c.fetchone()

        if prev_date_record is None:
            prev_date, prev_exp = '1900-01-01', expiry_date
        else:
            prev_date, prev_exp = max(prev_date_record[0], symbols_latest_date), \
                                  max(prev_date_record[1], symbols_latest_exp)
        c.execute(next_date_qry)
        next_date_record = c.fetchone()

        if next_date_record is None:
            next_date, next_exp = '2100-12-31', expiry_date
        else:
            next_date, next_exp = next_date_record[0], next_date_record[1]

        return {'prev_date': prev_date, 'prev_exp': prev_exp, 'next_date': next_date, 'next_exp': next_exp}

    def manage_missed_records(self, symbols=[], delta=0):
        '''
        Identify records missed while creating continuous contracts and insert them
        :param symbols: [list of symbols], no need to pass anything if for all symbols
        :return:
        '''
        print('start manage missed records')

        if len(symbols) == 0:  # no symbol passed, default to all symbols
            symbols = self.unique_symbols()

        # Identify symbol-date combinations which were available in tblDump but not included in tblFutures

        qry = '''SELECT tblDump.Symbol, tblDump.Date, tblDump.ExpiryDate, tblDump.VolumeLots
                   FROM tblDump LEFT OUTER JOIN tblFutures
                     ON tblDump.Symbol = tblFutures.Symbol
                    AND tblDump.Date = tblFutures.Date
                  WHERE tblFutures.date is NULL
                  ORDER BY tblDump.Symbol ASC, tblDump.ExpiryDate ASC, tblDump.Date ASC'''

        missed_records = pd.read_sql_query(qry, self.conn)
        select_missed_records = missed_records if len(symbols) == 0 \
            else missed_records[missed_records.Symbol.isin(symbols)]

        try:
            os.remove(self.selected_records_file)
            os.remove(self.eligible_records_file)
        except OSError:
            pass

        symbols_latest_date, symbols_latest_exp = dict(), dict()

        curr_symbol = ""
        c = self.conn.cursor()

        selected_records, eligible_records = pd.DataFrame(), pd.DataFrame()
        selected_records_isempty, eligible_records_isempty = True, True

        for symbol in select_missed_records['Symbol'].unique():
            symbol_expiry_qry = '''SELECT ExpiryDate FROM tblExpiries WHERE Symbol = "{}" 
                                    ORDER BY ExpiryDate ASC'''.format(symbol)
            symbol_expiries = pd.read_sql_query(symbol_expiry_qry, self.conn)

            symbols_latest_date[symbol] = '1900-01-01'
            symbols_latest_exp[symbol] = symbol_expiries['ExpiryDate'].tolist()[0]

            symbol_missed_records = select_missed_records[select_missed_records.Symbol == symbol]

            for expiry_date in symbol_missed_records['ExpiryDate'].unique():
                symbol_missed_records_for_expiry = \
                    symbol_missed_records[symbol_missed_records.ExpiryDate == expiry_date]
                if len(symbol_missed_records_for_expiry.index) == 0:
                    print('{}: skipping expiry date {}, symbol latest date {}'.format(symbol, expiry_date,
                                                                                      symbols_latest_date[symbol]))
                    continue

                all_dates = symbol_missed_records_for_expiry['Date'].unique()
                all_dates.sort()

                prev_next_dates = self.prev_and_next_dates(c, symbol, expiry_date,
                                                           symbols_latest_date[symbol], symbols_latest_exp[symbol],
                                                           all_dates[0], all_dates[len(all_dates) - 1])

                prev_date, prev_exp, next_date, next_exp = prev_next_dates['prev_date'], prev_next_dates['prev_exp'],\
                                                           prev_next_dates['next_date'], prev_next_dates['next_exp']

                if next_exp < expiry_date or prev_exp > expiry_date:
                    print('{}: skipping expiry date {}, prev exp {} next exp {}'.format(symbol, expiry_date,
                                                                                        prev_exp, next_exp))
                    continue
                else:
                    print('{}: checking expiry date {}, prev exp {} next exp {}'.format(symbol, expiry_date,
                                                                                        prev_exp, next_exp))

                prev_date_plus_1, next_date_minus_1 = dates.relativedate(prev_date, days=1), \
                                                      dates.relativedate(next_date, days=-1)

                selected_records_qry = '''SELECT * FROM tblFutures
                                                   WHERE Symbol = "{}" 
                                                     AND Date BETWEEN "{}" AND "{}"'''.format(
                    symbol, prev_date_plus_1, next_date_minus_1)

                selected_records_temp = pd.read_sql_query(selected_records_qry, self.conn)

                eligible_records_qry = '''SELECT Symbol, Date, Open, High, Low, Close, VolumeLots, OpenInterestLots, ExpiryDate 
                                            FROM tblDump
                                           WHERE Symbol = "{}" 
                                             AND Date BETWEEN "{}" AND "{}"
                                             AND ExpiryDate = "{}"'''.format(
                    symbol, prev_date_plus_1, next_date_minus_1, expiry_date)

                eligible_records_temp = pd.read_sql_query(eligible_records_qry, self.conn)

                for date in all_dates:
                    prev_next_dates2 = self.prev_and_next_dates(c, symbol, expiry_date,
                                                               symbols_latest_date[symbol], symbols_latest_exp[symbol],
                                                               date, date)

                    next_exp = prev_next_dates2['next_exp']

                    next_symbol_expiries = [d for d in symbol_expiries['ExpiryDate'].tolist() if d >= next_exp]
                    if len(next_symbol_expiries) >= 2:
                        next_next_expiry = next_symbol_expiries[1]
                    else:
                        if len(next_symbol_expiries) == 0:
                            next_next_expiry = expiry_date
                        else:
                            next_next_expiry = next_symbol_expiries[len(next_symbol_expiries) - 1]

                    eligible_records_temp2 = eligible_records_temp[(eligible_records_temp.Date >= date) &
                                                                   (eligible_records_temp.ExpiryDate <= next_next_expiry) &
                                                                   (eligible_records_temp.Date > symbols_latest_date[symbol])]

                    if eligible_records_temp2.empty:
                        print(date, 'eligible_records empty, skipping')
                        continue

                    start, end = eligible_records_temp2.iloc[0]['ExpiryDate'], \
                                 eligible_records_temp2.iloc[len(eligible_records_temp2.index) - 1]['ExpiryDate']

                    prev_next_dates3 = self.prev_and_next_dates(c, symbol, expiry_date,
                                                               symbols_latest_date[symbol], symbols_latest_exp[symbol],
                                                               start, end)

                    prev_exp, next_exp = prev_next_dates3['prev_exp'], prev_next_dates3['next_exp']

                    if next_exp < expiry_date or prev_exp > expiry_date:
                        print(date, 'next expiry is out of place, skipping', expiry_date, prev_exp, next_exp)
                        continue

                    selected_records_temp2 = selected_records_temp[(selected_records_temp.Date >= date) &
                                                                   (selected_records_temp.Date > symbols_latest_date[symbol])]

                    if len(eligible_records_temp2.index) > len(selected_records_temp2.index):
                        selected_records = pd.concat([selected_records, selected_records_temp2], axis=0)
                        eligible_records = pd.concat([eligible_records, eligible_records_temp2], axis=0)
                        symbols_latest_date[symbol] = \
                            eligible_records_temp2.iloc[len(eligible_records_temp2.index) - 1]['Date']
                        symbols_latest_exp[symbol] = \
                            eligible_records_temp2.iloc[len(eligible_records_temp2.index) - 1]['ExpiryDate']
                        print('eligible selected records range', eligible_records_temp2.iloc[0]['Date'],
                              eligible_records_temp2.iloc[len(eligible_records_temp2.index) - 1]['Date'],
                              len(eligible_records_temp2.index))
                        if len(selected_records_temp2.index) > 0:
                            print('replacing records range', selected_records_temp2.iloc[0]['Date'],
                                  selected_records_temp2.iloc[len(selected_records_temp2.index) - 1]['Date'],
                                  len(selected_records_temp2.index))
                        else:
                            print('replacing records range', 'NA', len(selected_records_temp2.index))

                        break

        selected_records.to_csv(self.selected_records_file, sep=',', index=False)
        eligible_records.to_csv(self.eligible_records_file, sep=',', index=False)

        c.close()

    def expiry_sanity_check(self):

        print('start sanity check')

        qry = '''SELECT * FROM tblFutures ORDER BY Symbol ASC, Date ASC'''

        records = pd.read_sql_query(qry, self.conn)

        symbol = ""
        for idx, row in records.iterrows():
            if row['Symbol'] != symbol:
                symbol = row['Symbol']
                prev_exp, prev_date = '1900-01-01', '1900-01-01'
            else:
                if row['ExpiryDate'] < prev_exp:
                    print(row['Symbol'], row['Date'], row['ExpiryDate'], prev_date, prev_exp)
                prev_exp, prev_date = row['ExpiryDate'], row['Date']

