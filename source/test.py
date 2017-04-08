"""
Created on Apr 2, 2017
@author: Souvik
@Program Function: Comparison test between adjusted and non-adjusted continuous contracts


"""

import os
import dates, utils
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



def read_history(symbols):

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()

    history = pd.DataFrame()
    for file in csv_files:
        df = pd.read_csv(file)

        records =  df.loc[df['Symbol'].isin(symbols)]
        history = pd.concat([history, records], axis=0)

    return history

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




os.chdir(PATH)
os.chdir(FORMATTED)

conn = sqlite3.connect(EOD_HIST) #EOD HIST UNADJUSTED, ALL DAYS, ALL FUTURES


os.chdir(VOL_CONTINUOUS)
trades = pd.read_csv(TRADES) #HLLH trades
#os.chdir('test/')


os.chdir(RATIO_ADJUSTED)
ra_hist = read_history(['GOLD'])  # Ratio-adjusted history

#ra_hist['Date'] = ra_hist['Date'].apply(dates.mm_dd_yyyy_to_yyyy_mm_dd)

print(ra_hist)


entry, prev_entry = None, None
tradecount, mismatch = 0, 0
for index, trade in trades.iterrows():

    #entry = signal_entries(trade['Date'], ua_hist, ra_hist)
    tradecount += 1
    entry = trade
    ra_entry = read_entry_price(trade['Date'], ra_hist)
    #print(ra_entry)
    #print('ra', ra_entry['Date'], ra_entry['Open'], ra_entry['Expiry Date'])
    if entry is not None and prev_entry is not None:
        ua_hist = read_db_hist(conn, entry['Symbol'], prev_entry['Date'], entry['Date'])
        ua_return = round(ra_entry['Open'] / prev_ra_entry['Open'], 2)
        ra_return = round(ua_hist[1][2] / ua_hist[0][2], 2)
        if ua_return != ra_return:
            mismatch += 1
            #print('ra', 'entry', prev_ra_entry['Date'], prev_ra_entry['Open'], prev_ra_entry['Expiry Date'],
            #  'exit', ra_entry['Date'], ra_entry['Open'], ra_entry['Expiry Date'],
            #  'return', round(ra_entry['Open'] / prev_ra_entry['Open'], 2))
            #print('ua', 'entry', ua_hist[0][1], ua_hist[0][2], ua_hist[0][10],
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
        #print(ua_hist)

    prev_entry = entry
    prev_ra_entry = ra_entry





