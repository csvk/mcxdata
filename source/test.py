"""
Created on Apr 2, 2017
@author: Souvik
@Program Function: Comparison test between adjusted and non-adjusted continuous contracts


"""

import os
import dates, utils
import csvhandler as csvh
import pandas as pd
import numpy as np
import sqlite3

PATH = 'C:/Users/Souvik/OneDrive/Python/mcxdata/data - vol - oi rollover/' # Laptop
FORMATTED = 'formatted/'
ROLLOVER_CLOSE = 'rollover_close.txt'
ROLLOVER_MULT = 'rollover_multipliers.txt'
CONTINUOUS = 'continuous/'
VOL_CONTINUOUS = 'continuous_vol/'
OI_CONTINUOUS = 'continuous_oi/'
RATIO_ADJUSTED = 'ratio_adjusted/'

EOD_HIST = 'db/DB.db'

TRADES = 'test/HLLH_GOLD.csv'

def read_history_all():

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()

    history = pd.DataFrame()
    for file in csv_files:
        df = pd.read_csv(file)

        #records =  df.loc[df['Symbol'] == symbol]
        history = pd.concat([history, df], axis=0)

    return history

def read_history(hist, symbol):

    records =  hist.loc[hist['Symbol'] == symbol]

    return records

def read_entry_price(date, hist):

    #date = dates.mm_dd_yyyy_to_yyyy_mm_dd(date)

    #print(date)

    #print(hist1)
    #print(hist2)

    h1 = np.where(hist['Date'] == date)[0][0] + 1
    #h2 = np.where(hist2['Date'] == date)[0][0] + 1

    #print(hist1[h1])
    #print(hist1[h2])
    #print(h1, h2)
    #print(hist1['Open'].iloc[h1])
    #print(hist2['Open'].iloc[h2])

    #return [[hist1['Open'].iloc[h1], hist1['Expiry Date'].iloc[h1]],
    #        [hist2['Open'].iloc[h1], hist2['Expiry Date'].iloc[h1]]]
    return hist.iloc[h1] #h1 #hist['Open'].iloc[h1]

def read_db_hist(conn, symbol, entry_date, exit_date):

    exit = """select * from EOD_HIST where SYMBOL = "{0}" and DATE > "{1}" and
        EXPIRY > "{1}" order by DATE asc, EXPIRY asc limit 1""".format(symbol, exit_date)

    #print(exit)

    c = conn.cursor()
    c.execute(exit)
    exit_row = c.fetchall()

    entry = """select * from EOD_HIST where SYMBOL = "{0}" and DATE > "{1}" and
            EXPIRY = "{2}" order by DATE asc, EXPIRY asc limit 1""".format(symbol, entry_date, exit_row[0][10])

    #print(entry)


    c.execute(entry)
    entry_row = c.fetchall()

    #print('ua', entry_row[0], exit_row[0])

    return [entry_row[0], exit_row[0]]


def ua_price(conn, symbol, date, expiry):

    qry = """select * from EOD_HIST where SYMBOL = "{}" and DATE = "{}" and
        EXPIRY = "{}" """.format(symbol, date, expiry)

    #print(exit)

    c = conn.cursor()
    c.execute(qry)
    day = c.fetchall()

    #print(day[0])

    #return [entry_row[0], exit_row[0]]
    return day[0]


def compare_test_1():
    """
    Compare test results for trades generated in Amibroker in ratio adjusted continuous data vs unadjusted data
    :return: None
    """


    os.chdir(PATH)
    os.chdir(FORMATTED)

    conn = sqlite3.connect(EOD_HIST)  # EOD HIST UNADJUSTED, ALL DAYS, ALL FUTURES

    os.chdir(VOL_CONTINUOUS)
    trades = pd.read_csv(TRADES)  # HLLH trades
    # os.chdir('test/')


    os.chdir(RATIO_ADJUSTED)
    ra_hist = read_history('GOLD')  # Ratio-adjusted history

    # ra_hist['Date'] = ra_hist['Date'].apply(dates.mm_dd_yyyy_to_yyyy_mm_dd)

    print(ra_hist)

    entry, prev_entry = None, None
    tradecount, mismatch = 0, 0
    for index, trade in trades.iterrows():

        # entry = signal_entries(trade['Date'], ua_hist, ra_hist)
        tradecount += 1
        entry = trade
        ra_entry = read_entry_price(trade['Date'], ra_hist)
        # print(ra_entry)
        # print('ra', ra_entry['Date'], ra_entry['Open'], ra_entry['Expiry Date'])
        if entry is not None and prev_entry is not None:
            ua_hist = read_db_hist(conn, entry['Symbol'], prev_entry['Date'], entry['Date'])
            ua_return = round(ra_entry['Open'] / prev_ra_entry['Open'], 2)
            ra_return = round(ua_hist[1][2] / ua_hist[0][2], 2)
            if ua_return != ra_return:
                mismatch += 1
                # print('ra', 'entry', prev_ra_entry['Date'], prev_ra_entry['Open'], prev_ra_entry['Expiry Date'],
                #  'exit', ra_entry['Date'], ra_entry['Open'], ra_entry['Expiry Date'],
                #  'return', round(ra_entry['Open'] / prev_ra_entry['Open'], 2))
                # print('ua', 'entry', ua_hist[0][1], ua_hist[0][2], ua_hist[0][10],
                #  'exit', ua_hist[1][1], ua_hist[1][2], ua_hist[1][10],
                #  'return', round(ua_hist[1][2] / ua_hist[0][2], 2))
                print('{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(
                    'ra', 'entry', prev_ra_entry['Date'], prev_ra_entry['Open'], prev_ra_entry['Expiry Date'],
                    'exit', ra_entry['Date'], ra_entry['Open'], ra_entry['Expiry Date'],
                    'return', round(ra_entry['Open'] / prev_ra_entry['Open'], 2),
                    'ua', 'entry', ua_hist[0][1], ua_hist[0][2], ua_hist[0][10],
                    'exit', ua_hist[1][1], ua_hist[1][2], ua_hist[1][10],
                    'return', round(ua_hist[1][2] / ua_hist[0][2], 2)))
            print(tradecount, 'trades', mismatch, 'mismatches')
            # print(ua_hist)

        prev_entry = entry
        prev_ra_entry = ra_entry


def compare_test_2(conn, hist, symbol):
    """
    Compare test results for trade entry on first day of expiry in ratio adjusted continuous data and exit on last day
    of expiry: between ratio adjusted continuous data vs unadjusted data
    :return: None
    """

    ra_hist = read_history(hist, symbol)  # Ratio-adjusted history

    prev_day = None
    #print('{},{},{},{},{}'.format('Signal', 'Date', 'Open', 'Close', 'Expiry Date'))
    #ra_signals = ['{},{},{},{},{}'.format('Signal', 'Date', 'Open', 'Close', 'Expiry Date')]
    #ua_signals = ['{},{},{},{},{}'.format('Signal', 'Date', 'Open', 'Close', 'Expiry Date')]
    ra_signals_lst = [['Signal', 'Symbol', 'Date', 'Open', 'Close', 'Expiry Date']]
    ua_signals_lst = [['Signal', 'Symbol', 'Date', 'Open', 'Close', 'Expiry Date']]

    for index, day in ra_hist.iterrows():
        if prev_day is not None:
            if day['Expiry Date'] != prev_day['Expiry Date']:
                #ra_signals.append('{},{},{},{},{}'.format('exit', prev_day['Date'], prev_day['Open'], prev_day['Close'],
                #                                          prev_day['Expiry Date']))
                #ra_signals.append('{},{},{},{},{}'.format('entry', day['Date'], day['Open'], day['Close'],
                #                                          day['Expiry Date']))
                ra_signals_lst.append(['exit', symbol, prev_day['Date'], prev_day['Open'],
                                       prev_day['Close'], prev_day['Expiry Date']])
                ra_signals_lst.append(['entry', symbol, day['Date'], day['Open'], day['Close'],
                                                          day['Expiry Date']])

                ua_prc = ua_price(conn, symbol, prev_day['Date'], prev_day['Expiry Date'])
                ua_signals_lst.append(['exit', ua_prc[0], ua_prc[1], ua_prc[2], ua_prc[5], ua_prc[10]])
                ua_prc = ua_price(conn, symbol, day['Date'], day['Expiry Date'])
                ua_signals_lst.append(['entry', ua_prc[0], ua_prc[1], ua_prc[2], ua_prc[5], ua_prc[10]])
                #print('{},{},{},{},{}'.format('exit', prev_day['Date'], prev_day['Open'], prev_day['Close'], prev_day['Expiry Date']))
                #print('{},{},{},{},{}'.format('entry', day['Date'], day['Open'], day['Close'], day['Expiry Date']))
        prev_day = day

    #print(ra_signals_lst)
    #print(ua_signals_lst)



    signals, mismatch, signalmismatch, zeroerror = 0, 0, 0, 0
    messages = []
    for i in range(2, len(ra_signals_lst)):
        if ra_signals_lst[i][0] == 'exit' and ua_signals_lst[i][0] == 'exit':
            signals += 1
            if ra_signals_lst[i][3] == 0 or ua_signals_lst[i][3] == 0:
                messages.append([symbol, ra_signals_lst[i][2], 'divided by zero error', ra_signals_lst[i][3],
                                 ua_signals_lst[i][3]])
                zeroerror += 1
            else:
                ra_growth = round(ra_signals_lst[i][4] / ra_signals_lst[i][3], 4)
                ua_growth = round(ua_signals_lst[i][4] / ua_signals_lst[i][3], 4)
                if ra_growth != ua_growth:
                    messages.append([symbol, ra_signals_lst[i][2], 'signal growth mismatch', ra_growth, ua_growth])
                    mismatch += 1
                else:
                    pass
                    # print(ra_signals_lst[i][1], 'signal growth match')
        elif ra_signals_lst[i][0] == 'entry' and ua_signals_lst[i][0] == 'entry':
            pass
        else:
            print(symbol, ra_signals_lst[i][2], 'Signal Mismatch')
            signalmismatch += 1

    print(symbol, signals, 'signals tested, found', mismatch, 'mismatches                  ,',
          zeroerror, 'divisible by zero errors,', signalmismatch, 'signal mismatches')

    return messages


os.chdir(PATH)
os.chdir(FORMATTED)
exp = csvh.read_expiry_hist()
print(exp)


conn = sqlite3.connect(EOD_HIST)  # EOD HIST UNADJUSTED, ALL DAYS, ALL FUTURES

os.chdir(VOL_CONTINUOUS)

os.chdir(RATIO_ADJUSTED)
#os.chdir('test/')


hist = read_history_all()

#compare_test_2(conn, hist, 'GOLD')

messages = []
for key, data in exp['expiry_dates'].items():
    messages.append(compare_test_2(conn, hist, str.strip(key)))


for message in messages:
    for msg in message:
        print(msg)

