"""
Created on Apr 11, 2017
@author: Souvik
@Program Function: Data sanity check


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


def check_volume(df, symbols):

    #symbols_set = set(symbols)
    valid_symbols = []
    for symbol in symbols:
        df_sel = df.loc[(df['Symbol'] == symbol) & (df['Volume'] > 0)]
        if not df_sel.empty:
            valid_symbols.append(symbol)

    return valid_symbols


def daily_signal_check():

    formatted_files = [f for f in os.listdir(FORMATTED) if f.endswith('.csv')]
    formatted_files.sort()


    for file in formatted_files:

        date = file[0:10]
        f_file = FORMATTED + file
        c_file = FORMATTED + VOL_CONTINUOUS + file
        ra_file = FORMATTED + VOL_CONTINUOUS + RATIO_ADJUSTED + file
        #print(date, f_file, c_file, ra_file)

        f_df = pd.read_csv(f_file)
        c_df = pd.read_csv(c_file)
        ra_df = pd.read_csv(ra_file)

        f_df['Symbol'] = f_df['Symbol'].apply(str.strip)
        f_symbols = f_df['Symbol'].unique()
        f_vol_symbols = check_volume(f_df, f_df['Symbol'].unique())
        f_novol_symbols = list(set(f_symbols) - set(f_vol_symbols))
        c_symbols = c_df['Symbol'].unique()
        ra_symbols = ra_df['Symbol'].unique()

        fvset, fnvset, cset, raset =  set(f_vol_symbols), set(f_novol_symbols), set(c_symbols), set(ra_symbols)
        if len(list(fvset - cset)) > 1:
            print(date, list(fvset - cset), 'symbols not in continuous')
        if len(list(cset - raset)) > 1:
            print(date, list(cset - raset), 'symbols not in ratio adjusted')
        if len(list(cset - fvset)) > 1:
            print(date, list(cset - fvset), 'symbols extra in continuous')
        if len(list(raset - cset)) > 1:
            print(date, list(raset - cset), 'symbols extra in ratio adjusted')




        """
        print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        print(f_symbols)
        print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        print(c_symbols)
        print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        print(ra_symbols)
        print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        """



def daily_expiry_date_check():
    formatted_files = [f for f in os.listdir(FORMATTED) if f.endswith('.csv')]
    formatted_files.sort()

    for file in formatted_files:
        f_file = FORMATTED + file
        c_file = FORMATTED + VOL_CONTINUOUS + file
        ra_file = FORMATTED + VOL_CONTINUOUS + RATIO_ADJUSTED + file
        print(f_file, c_file, ra_file)
        f_df = pd.read_csv(f_file)
        c_df = pd.read_csv(c_file)
        ra_df = pd.read_csv(ra_file)
        print(file, f_df['Expiry Date'][0], c_df['Expiry Date'][0], ra_df['Expiry Date'][0])








os.chdir(PATH)
#os.chdir('test')
daily_signal_check()



