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
from collections import OrderedDict
#from pympler.tracker import SummaryTracker, ObjectTracker
import gc


class DataDB:
    """ Historical Bhavcopy data"""

    # constants

    INSTRUMENT_NAME = 'FUTCOM'
    SELECTED_RECORDS_FILE = 'selected_records.csv'
    ELIGIBLE_RECORDS_FILE = 'eligible_records.csv'
    DUPLICATE_RECORDS_FILE = 'duplicate_records.csv'
    DUPLICATE_IGNORED_FILE = 'duplicate_ignored.csv'
    APPENDED_RECORDS_FILE = 'appended_records.csv'
    AMIBROKER_FILE = 'ami_data.csv'
    AMIBROKER_ADJUSTED_FILE = 'ami_data_adjusted.csv'
    FORMATTED = 'formatted/'

    # variables
    trading_day_idx = dict()
    trading_day_idx_rev = dict()

    def set_trading_day_idx(self):
        """
        Populate trading_day_idx, trading_day_idx_rev dictionary
        """

        qry = '''SELECT DISTINCT Date FROM tblDump 
                  WHERE InstrumentName = "{}" ORDER BY Date'''.format(self.INSTRUMENT_NAME)

        c = self.conn.cursor()
        c.execute(qry)
        rows = c.fetchall()
        c.close()

        dates = [row[0] for row in rows]
        date_idx = [i + 1 for i in range(0, len(rows))]

        self.trading_day_idx, self.trading_day_idx_rev = dict(zip(dates, date_idx)), dict(zip(date_idx, dates))

    def __init__(self, db, type='FUTCOM'):

        # variables

        self.INSTRUMENT_NAME = type    

        print('Opening Bhavcopy database {}...'.format(db))
        self.conn = sqlite3.connect(db)
        self.engine = create_engine('sqlite:///{}'.format(db))

        self.set_trading_day_idx()

    def __del__(self):

        print('Closing DB connection..')
        self.conn.close()

    def dump_record_count(self):

        c = self.conn.cursor()
        c.execute('''SELECT COUNT(*) FROM tblDump WHERE InstrumentName = "{}"'''.format(self.INSTRUMENT_NAME))
        rows = c.fetchall()
        c.close()

        print("Total number of records in the data dump: {}".format(rows[0][0]))

    def unique_symbols(self, table='tblDump'):

        qry = '''SELECT DISTINCT Symbol FROM "{}" WHERE InstrumentName = "{}"'''.format(table, self.INSTRUMENT_NAME)

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
                           WHERE InstrumentName = "{}"'''.format(self.INSTRUMENT_NAME)

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

    def symbol_records(self, symbol, start='1900-01-01', end='2100-12-31'):

        qry = '''SELECT Symbol, Date, Open, High, Low, Close, VolumeLots, OpenInterestLots, ExpiryDate 
                   FROM tblDump
                  WHERE Symbol = "{}"
                    AND InstrumentName = "{}"
                    AND Date BETWEEN "{}" AND "{}"
                  ORDER BY Symbol ASC, Date ASC, ExpiryDate ASC'''.format(symbol, self.INSTRUMENT_NAME, start, end)

        df = pd.read_sql_query(qry, self.conn)

        return df

    def insert_records(self, df, table_name):
        """
        Insert passed records into tblFutures
        """

        df.to_sql(table_name, self.engine, index=False, if_exists='append')

    def create_continuous_contracts(self, symbols=[], delta=0):
        """
        Create continuous contracts with rollover day on delta trading days from expiry
        delta = 0 means rollover happens on expiry day
        :param symbols:
        [list of symbols], no need to pass anything if for all symbols
        delta: delta days for rollover before expiry
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
                #curr_expiry = expiries[expiry_idx]
                curr_expiry_idx = self.trading_day(expiry)
                sel_records = df.loc[(df['ExpiryDate'] == expiry) &
                                     (df['TradingDay'] < curr_expiry_idx - delta ) &
                                     (df['TradingDay'] >= next_trading_day_idx)]
                records = pd.concat([records, sel_records], axis=0)

                next_trading_day_idx = curr_expiry_idx - delta
                expiry_idx = expiry_idx + 1

        df_insert = records.drop(['TradingDay'], axis=1)

        df_unique = df_insert.drop_duplicates(['Symbol', 'Date'], keep=False)
        df_duplicate = df_insert[df_insert.duplicated(['Symbol', 'Date'], keep=False)]

        try:
            os.remove(self.DUPLICATE_RECORDS_FILE)
        except OSError:
            pass
        if len(df_duplicate.index) > 0:
            df_duplicate.to_csv(self.DUPLICATE_RECORDS_FILE, sep=',', index=False)

        self.insert_records(df_unique, table_name='tblFutures')

    def prev_and_next_dates(self, c, symbol, expiry_date, symbols_latest_date, symbols_latest_exp, start, end):
        '''

        :param c: DB connection
        :param symbol: symbol
        :param expiry_date: current expiry date
        :param symbols_latest_date: last date already selected for symbol
        :param symbols_latest_exp: expiry_date for last date already selected for symbol
        :param start: date range start
        :param end: date range end
        :return: {prev and next dates and expiries before and after date range}
        '''

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
                  WHERE tblFutures.Date is NULL
                    AND tblDump.InstrumentName = "{}"
                  ORDER BY tblDump.Symbol ASC, tblDump.ExpiryDate ASC, tblDump.Date ASC'''.format(self.INSTRUMENT_NAME)

        missed_records = pd.read_sql_query(qry, self.conn)
        select_missed_records = missed_records if len(symbols) == 0 \
            else missed_records[missed_records.Symbol.isin(symbols)]

        try:
            os.remove(self.SELECTED_RECORDS_FILE)
            os.remove(self.ELIGIBLE_RECORDS_FILE)
        except OSError:
            pass

        symbols_latest_date, symbols_latest_exp = dict(), dict()

        c = self.conn.cursor()

        selected_records, eligible_records = pd.DataFrame(), pd.DataFrame()

        # Loop through symbols
        for symbol in select_missed_records['Symbol'].unique():
            symbol_expiry_qry = '''SELECT ExpiryDate FROM tblExpiries WHERE Symbol = "{}" 
                                    ORDER BY ExpiryDate ASC'''.format(symbol)
            symbol_expiries = pd.read_sql_query(symbol_expiry_qry, self.conn)

            symbols_latest_date[symbol] = '1900-01-01'
            symbols_latest_exp[symbol] = symbol_expiries['ExpiryDate'].tolist()[0]

            symbol_missed_records = select_missed_records[select_missed_records.Symbol == symbol]

            # Loop through expiry_dates for the symbol
            for expiry_date in symbol_missed_records['ExpiryDate'].unique():
                symbol_missed_records_for_expiry = \
                    symbol_missed_records[symbol_missed_records.ExpiryDate == expiry_date]
                if len(symbol_missed_records_for_expiry.index) == 0:
                    print('{}: skipping expiry date {}, symbol latest date {}'.format(symbol, expiry_date,
                                                                                      symbols_latest_date[symbol]))
                    continue

                # Sort all dates selected
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

                # Select records for compare and deletion if needed
                selected_records_qry = '''SELECT * FROM tblFutures
                                                   WHERE Symbol = "{}" 
                                                     AND Date BETWEEN "{}" AND "{}"'''.format(
                    symbol, prev_date_plus_1, next_date_minus_1)

                selected_records_temp = pd.read_sql_query(selected_records_qry, self.conn)

                # Select eligible records
                eligible_records_qry = '''SELECT Symbol, Date, Open, High, Low, Close, VolumeLots, OpenInterestLots, ExpiryDate 
                                            FROM tblDump
                                           WHERE Symbol = "{}" 
                                             AND Date BETWEEN "{}" AND "{}"
                                             AND ExpiryDate = "{}"'''.format(
                    symbol, prev_date_plus_1, next_date_minus_1, expiry_date)

                eligible_records_temp = pd.read_sql_query(eligible_records_qry, self.conn)

                # Loop through missing dates
                for date in all_dates:
                    prev_next_dates2 = self.prev_and_next_dates(c, symbol, expiry_date,
                                                               symbols_latest_date[symbol], symbols_latest_exp[symbol],
                                                               date, date)

                    next_exp = prev_next_dates2['next_exp']

                    next_symbol_expiries = [d for d in symbol_expiries['ExpiryDate'].tolist() if d >= next_exp]

                    # Find expiry after the next to make sure records from too fare away are not identified as eligible
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

        selected_records.to_csv(self.SELECTED_RECORDS_FILE, sep=',', index=False)
        eligible_records.to_csv(self.ELIGIBLE_RECORDS_FILE, sep=',', index=False)

        c.close()

    def update_continuous_contract(self, symbols=[]):

        print('start update continuous contract')

        try:
            selected_records = pd.read_csv(self.SELECTED_RECORDS_FILE)
            eligible_records = pd.read_csv(self.ELIGIBLE_RECORDS_FILE)
        except pd.errors.EmptyDataError:
            print('Empty file, skipping update')
            return 0

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
            os.remove(self.DUPLICATE_IGNORED_FILE)
        except OSError:
            pass
        duplicate_ignored.to_csv(self.DUPLICATE_IGNORED_FILE, sep=',', index=False)

        self.conn.commit()
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

    def load_table_from_csv(self, csv_path, table_name='tblDumpStaging'):

        truncate_query = '''DELETE FROM {}'''.format(table_name)

        c = self.conn.cursor()
        c.execute(truncate_query)
        self.conn.commit()

        csv_files = [f for f in os.listdir(csv_path + self.FORMATTED) if f.endswith('.csv')]
        csv_files.sort()

        print('Initiating loading of {} files'.format(len(csv_files)))

        read_count, write_count = 0, 0

        for file in csv_files:
            df_file = pd.read_csv(csv_path + self.FORMATTED + file)
            read_count = read_count + len(df_file.index)

            for idx, row in df_file.iterrows():
                if row['Date'] <= '2017-05-12':
                    insert_row = (row['Date'], 'FUTCOM', row['Symbol'], row['Expiry Date'],
                                  '-', 0, row['Open'], row['High'], row['Low'],
                                  row['Close'], row['Previous Close'], row['Volume'], row["Volume(In 000's)"],
                                  row['Value'], row['Open Interest'])
                else:
                    insert_row = (row['Date'], row['Instrument Name'], row['Symbol'], row['Expiry Date'],
                                  row['Option Type'], row['Strike Price'], row['Open'], row['High'], row['Low'],
                                  row['Close'], row['Previous Close'], row['Volume(Lots)'], row["Volume(In 000's)"],
                                  row['Value(Lacs)'], row['Open Interest(Lots)'])
                c.execute('''INSERT INTO {} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''.format(table_name),
                          insert_row)

            if read_count - write_count > 20000:
                self.conn.commit()
                write_count = read_count
                print('inserted {} records till now'.format(write_count))

        self.conn.commit()
        write_count = read_count
        c.close()

        print('{} files processed, {} records inserted'.format(len(csv_files), write_count))

    def process_staging_data(self):

        c = self.conn.cursor()

        date_start_qry = '''SELECT Date FROM tblDumpStaging ORDER BY Date ASC'''
        date_end_qry = '''SELECT Date FROM tblDumpStaging ORDER BY Date DESC'''

        c.execute(date_start_qry)
        date_start_record = c.fetchone()
        c.execute(date_end_qry)
        date_end_record = c.fetchone()

        if date_start_record is None:
            print('staging table empty')
            return None

        start_date, end_date = date_start_record[0],date_end_record[0]

        print('Processing staging records from {} to {}'.format(start_date, end_date))

        qry = '''SELECT * FROM tblDumpStaging'''

        records = pd.read_sql_query(qry, self.conn)

        for idx, row in records.iterrows():
            insert_row = (row['Date'], row['InstrumentName'], row['Symbol'], row['ExpiryDate'],
                          row['OptionType'], row['StrikePrice'], row['Open'], row['High'], row['Low'],
                          row['Close'], row['PreviousClose'], row['VolumeLots'], row["VolumeThousands"],
                          row['Value'], row['OpenInterestLots'])

            c.execute('''INSERT INTO tblDump VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', insert_row)

        self.conn.commit()
        c.close()
        print('loaded {} records from staging to main dump'.format(len(records.index)))

        self.write_expiries()
        self.set_trading_day_idx()

        return {'start': start_date, 'end': end_date}

    def append_continuous_contracts(self, start_date, symbols=[], delta=0):
        """
        Append continuous contracts with rollover day on delta trading days from expiry
        delta = 0 means rollover happens on expiry day
        :param symbols:
        start_date: start date in tblDumpStaging (start for which append is needed
        [list of symbols]: no need to pass anything if for all symbols
        delta: delta days for rollover before expiry
        :return:
        """

        if len(symbols) == 0: # no symbol passed, default to all symbols
            symbols = self.unique_symbols(table='tblDumpStaging')

        c = self.conn.cursor()

        records = pd.DataFrame()
        for symbol in symbols:
            print("Appending for {}".format(symbol))
            expiries = self.expiry_history(symbol)

            df = self.symbol_records(symbol, start=start_date)
            print('expiries', min(expiries), max(expiries), 'records', df['Date'].min(), df['Date'].max())

            df['TradingDay'] = [self.trading_day_idx[date] for date in df['Date']]

            latest_expiry_qry = '''SELECT ExpiryDate FROM tblFutures 
                                  WHERE Symbol = "{}" 
                                  ORDER BY Date DESC'''.format(symbol)

            c.execute(latest_expiry_qry)
            latest_expiry_record = c.fetchone()

            if latest_expiry_record is None:
                print('{}: symbol not found in tblFutures'.format(symbol))
                latest_expiry = '1900-01-01'
            else:
                latest_expiry = latest_expiry_record[0]
                print(symbol, ': latest expiry', latest_expiry)

            next_trading_day_idx = 0

            eligible_expiries = [expiry for expiry in expiries if expiry >= latest_expiry]
            eligible_expiries.sort()

            for expiry in eligible_expiries:
                print('processing expiry', expiry)
                curr_expiry_idx = self.trading_day(expiry)
                sel_records = df.loc[(df['ExpiryDate'] == expiry) &
                                     (df['TradingDay'] < curr_expiry_idx - delta) &
                                     (df['TradingDay'] >= next_trading_day_idx)]
                records = pd.concat([records, sel_records], axis=0)

                next_trading_day_idx = curr_expiry_idx - delta

        df_insert = records.drop(['TradingDay'], axis=1)

        df_unique = df_insert.drop_duplicates(['Symbol', 'Date'], keep=False)
        df_duplicate = df_insert[df_insert.duplicated(['Symbol', 'Date'], keep=False)]

        try:
            os.remove(self.DUPLICATE_RECORDS_FILE)
            os.remove(self.APPENDED_RECORDS_FILE)
        except OSError:
            pass

        if len(df_duplicate.index) > 0:
            df_duplicate.to_csv(self.DUPLICATE_RECORDS_FILE, sep=',', index=False)

        if len(df_unique.index) > 0:
            df_unique.to_csv(self.APPENDED_RECORDS_FILE, sep=',', index=False)

        duplicate_ignored = pd.DataFrame()
        for idx, row in df_unique.iterrows():
            if row['Symbol'] not in symbols and symbols != []:
                continue

            insert_row = (row['Symbol'], row['Date'], row['Open'], row['High'], row['Low'], row['Close'],
                          row['VolumeLots'], row['OpenInterestLots'], row['ExpiryDate'])

            check_qry = '''SELECT * FROM tblFutures WHERE Symbol = "{}" AND Date = "{}"'''.format(row['Symbol'],
                                                                                                  row['Date'])

            duplicate_record = pd.read_sql_query(check_qry, self.conn)

            if duplicate_record.empty:
                c.execute('''INSERT INTO tblFutures VALUES (?,?,?,?,?,?,?,?,?)''', insert_row)
                pass
            else:
                duplicate_record['Flag'] = "Dup"
                row['Flag'] = "Ign"
                row_frame = row.to_frame().T

                duplicate_ignored = pd.concat([duplicate_ignored, duplicate_record, row_frame], axis=0)

        try:
            os.remove(self.DUPLICATE_IGNORED_FILE)
        except OSError:
            pass
        duplicate_ignored.to_csv(self.DUPLICATE_IGNORED_FILE, sep=',', index=False)

        self.conn.commit()
        c.close()

    def calculate_historical_multipliers(self, type='append', symbols=[]):
        """
        Calculate historical rollover multipliers
        :param symbols: [symbol1, symbol2,...]
        type: 'append' if multipliers to be appended, 'refresh, if multipliers to be refreshed
        :return:
        """
        print('start calculate multipliers')

        if type == 'refresh':
            print('refreshing tblMultipliers')
            df = pd.DataFrame()
        else:
            print('appending tblMultipliers')
            qry = '''SELECT * FROM tblMultipliers'''
            df = pd.read_sql_query(qry, self.conn)

        if len(symbols) == 0:  # no symbol passed, default to all symbols
            symbols = self.unique_symbols()

        #c = self.conn.cursor()

        # Loop through symbols
        for symbol in symbols:
            print(symbol, 'calculating multipliers')

            if type == 'refresh':
                prev_expiry = prev_date = '1900-01-01'
                resultant_multiplier = 1
            else: # type == 'append'
                prev_expiry_qry = '''SELECT NextExpiry PrevExpiry, RolloverDate PrevDate, ResultantMultiplier
                                       FROM tblMultipliers WHERE Symbol = "{}"
                                      ORDER BY RolloverDate DESC LIMIT 1'''.format(symbol)
                prev_expiry_record = pd.read_sql_query(prev_expiry_qry, self.conn)
                if len(prev_expiry_record.index) > 0:
                    prev_expiry = prev_expiry_record.iloc[0]['PrevExpiry']
                    prev_date = prev_expiry_record.iloc[0]['PrevDate']
                    resultant_multiplier = prev_expiry_record.iloc[0]['ResultantMultiplier']
                else:
                    prev_expiry = prev_date = '1900-01-01'
                    resultant_multiplier = 1

            # print(symbol, prev_date, prev_expiry)
            symbol_records_qry = '''SELECT * FROM tblFutures WHERE Symbol = "{}" AND Date > "{}"
                                    ORDER BY ExpiryDate ASC'''.format(symbol, prev_date)
            symbol_records = pd.read_sql_query(symbol_records_qry, self.conn)

            for idx, row in symbol_records.iterrows():
                if prev_expiry == row['ExpiryDate']:
                    continue

                if prev_expiry == '1900-01-01':
                    prev_expiry = row['ExpiryDate']
                    continue

                earlier_day_qry = '''SELECT F.Date, F.Close FuturesClose, D.Close DumpClose
                           FROM tblDump F JOIN tblDump D
                             ON F.Symbol = D.Symbol
                            AND F.Date = D.Date
                          WHERE F.Symbol = "{}"
                            AND F.Date < "{}"
                            AND F.InstrumentName = "{}"
                            AND D.InstrumentName = "{}"
                            AND F.ExpiryDate = "{}"
                            AND D.ExpiryDate = "{}"
                          ORDER BY F.Date DESC
                          LIMIT 1'''.format(symbol, row['Date'], self.INSTRUMENT_NAME, self.INSTRUMENT_NAME,
                                            row['ExpiryDate'], prev_expiry)

                #multiplier_record = pd.read_sql_query(same_day_qry, self.conn)

                #if len(multiplier_record.index) == 0:
                multiplier_record = pd.read_sql_query(earlier_day_qry, self.conn)

                if len(multiplier_record.index) > 0:
                    multiplier_calc_date = multiplier_record.iloc[0]['Date']
                    multiplier_calc_type = "Same Day" if multiplier_calc_date == row['Date'] else "Before Rollover"
                    futures_close, dump_close = multiplier_record.iloc[0]['FuturesClose'], \
                                              multiplier_record.iloc[0]['DumpClose']
                    if futures_close == 0 or dump_close == 0:
                        multiplier_calc_type = "Zero Close Default 1"
                        multiplier = 1
                        # resultant_multiplier remains same
                    else:
                        multiplier = dump_close / futures_close
                        resultant_multiplier = resultant_multiplier * multiplier
                else:
                    multiplier_calc_date = row['Date']
                    multiplier_calc_type = "Not Found Default 1"
                    futures_close = dump_close = None
                    multiplier = 1
                    # resultant_multiplier remains same

                days_between = self.trading_day(row['Date']) - self.trading_day(multiplier_calc_date)

                insert_row = [('Symbol', symbol), ('RolloverDate', row['Date']), ('PreviousExpiry', prev_expiry),
                              ('NextExpiry', row['ExpiryDate']), ('DumpClose', dump_close),
                              ('FuturesClose', futures_close), ('MultiplierCalcType', multiplier_calc_type),
                              ('MultiplierCalcDate', multiplier_calc_date), ('DaysBetweenCalcRollover', days_between),
                              ('Multiplier', multiplier), ('ResultantMultiplier', resultant_multiplier)]

                df = pd.concat([df, pd.DataFrame([OrderedDict(insert_row)])], axis=0)

                prev_expiry = row['ExpiryDate']

        #print(df[df.duplicated(['Symbol', 'RolloverDate'], keep=False)])
        df.to_csv('multipliers.csv', sep=',', index=False)

        truncate_query = '''DELETE FROM tblMultipliers'''

        c = self.conn.cursor()
        c.execute(truncate_query)
        self.conn.commit()

        self.insert_records(df, table_name='tblMultipliers')

    def create_adjusted_contract(self, symbols=[]):

        print('start create adjusted contract')

        if len(symbols) == 0:  # no symbol passed, default to all symbols
            symbols = self.unique_symbols()

        df = pd.DataFrame()

        for symbol in symbols:
            print('creating adjusted contract for', symbol)

            # Find records which are available in tblFutures but not in tblContract
            qry = '''SELECT F.* 
                       FROM tblFutures F LEFT OUTER JOIN tblContract C
                         ON F.Symbol = C.Symbol
                        AND F.Date = C.Date
                      WHERE F.Symbol = "{}"
                        AND C.Date is NULL
                      ORDER BY F.Date ASC'''.format(symbol)

            symbol_records = pd.read_sql_query(qry, self.conn)

            multiplier_qry = '''SELECT * 
                                  FROM tblMultipliers
                                 WHERE Symbol = "{}"
                                 ORDER BY RolloverDate ASC'''.format(symbol)
            multiplier_records = pd.read_sql_query(multiplier_qry, self.conn)

            prev_expiry = '1900-01-01'
            m_idx = 0
            multiplier = 1
            adjusted_open, adjusted_high, adjusted_low, adjusted_close, multipliers = [], [], [], [], []
            count = 1
            for idx, row in symbol_records.iterrows():
                #print('count', count)
                #print(multiplier_records.iloc[m_idx])
                #print(len(multiplier_records), m_idx)
                #print(multiplier_records)
                if len(multiplier_records.index) == 0:
                    multiplier = 1
                elif prev_expiry == '1900-01-01':
                    selected_multiplier_record = multiplier_records[multiplier_records.RolloverDate >= row['Date']]
                    if len(selected_multiplier_record.index) == 0:
                        multiplier = multiplier_records.iloc[-1]['ResultantMultiplier'] # Last record
                        m_idx = multiplier_records.axes[0][-1]  # Axis of last record
                    else:
                        multiplier = selected_multiplier_record.iloc[0]['ResultantMultiplier'] # First selected record
                        m_idx = selected_multiplier_record.axes[0][0]  # Axis of first record
                elif row['Date'] >= multiplier_records.iloc[m_idx]['RolloverDate']:
                    multiplier = multiplier_records.iloc[m_idx]['ResultantMultiplier']
                    if prev_expiry != multiplier_records.iloc[m_idx]['PreviousExpiry'] \
                            and m_idx + 1 < len(multiplier_records):
                        print(symbol, row['Date'], prev_expiry, multiplier_records.iloc[m_idx]['PreviousExpiry'],
                              'expiry date mismatch')
                    if m_idx + 1 < len(multiplier_records):
                        m_idx = m_idx + 1
                #if m_idx > 2:
                #    continue

                #print(adjusted_open)
                adjusted_open.append(round(row['Open'] * multiplier, 2))
                #print('open', len(adjusted_open), row['Open'], row['Open'] * multiplier, adjusted_open)
                adjusted_high.append(round(row['High'] * multiplier, 2))
                adjusted_low.append(round(row['Low'] * multiplier, 2))
                adjusted_close.append(round(row['Close'] * multiplier, 2))
                multipliers.append(multiplier)

                prev_expiry = row['ExpiryDate']
                count = count + 1

            #print(len(symbol_records))
            #print(len(adjusted_open))

            symbol_records['AdjustedOpen'] = adjusted_open
            symbol_records['AdjustedHigh'] = adjusted_high
            symbol_records['AdjustedLow'] = adjusted_low
            symbol_records['AdjustedClose'] = adjusted_close
            symbol_records['Multiplier'] = multipliers

            #print(symbol_records)
            df = pd.concat([df, symbol_records], axis=0)

        # print(df[df.duplicated(['Symbol', 'Date'], keep=False)])
        df.to_csv('contract.csv', sep=',', index=False)

        self.insert_records(df, table_name='tblContract')

    def create_amibroker_import_files(self, path, start_date='1900-01-01'):

        qry = '''SELECT * FROM tblContract WHERE Date >= "{}"'''.format(start_date)

        all_fields = pd.read_sql_query(qry, self.conn)
        print(all_fields)

        last_date = all_fields.iloc[-1]['Date']

        all_fields['ExpiryDate'] = all_fields['ExpiryDate'].apply(dates.yyyy_mm_dd_to_yyyymmdd)  # Update Expiry Date Format
        unadjusted = all_fields[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'VolumeLots', 'OpenInterestLots',
                        'ExpiryDate']]
        unadjusted.to_csv(path + last_date + '.csv', sep=',', index=False)

        all_fields['Symbol'] = all_fields['Symbol'].apply(lambda s: s + '-A')
        adjusted = all_fields[['Symbol', 'Date', 'AdjustedOpen', 'AdjustedHigh', 'AdjustedLow', 'AdjustedClose']]
        adjusted.to_csv(path + last_date + '.A.csv', sep=',', index=False)













