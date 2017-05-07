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
#PATH = 'C:/Users/Souvik/OneDrive/Python/mcxdata/data - TDM rollover - 0-I/' # Laptop
FORMATTED = 'formatted/'
ROLLOVER_CLOSE = 'rollover_close.txt'
ROLLOVER_MULT = 'rollover_multipliers.txt'
CONTINUOUS = 'continuous/'
VOL_CONTINUOUS = 'continuous_vol/'
OI_CONTINUOUS = 'continuous_oi/'
RATIO_ADJUSTED = 'ratio_adjusted/'
INTERMEDIATE = 'intermediate'
FINAL = 'final'

EOD_HIST = 'db/DB.db'


def check_volume(df, symbols):

    #symbols_set = set(symbols)
    valid_symbols = []
    for symbol in symbols:
        df_sel = df.loc[(df['Symbol'] == symbol) & (df['Volume'] > 0)]
        if not df_sel.empty:
            valid_symbols.append(symbol)

    return valid_symbols


def daily_symbol_check():
    """
    Test symbol selected or not
    :return:
    """

    formatted_files = [f for f in os.listdir(FORMATTED) if f.endswith('.csv')]
    formatted_files.sort()


    for file in formatted_files:

        date = file[0:10]
        f_file = FORMATTED + file

        c_file = FORMATTED + CONTINUOUS + file
        ra_file = FORMATTED + CONTINUOUS + RATIO_ADJUSTED + file

        f_df = pd.read_csv(f_file)
        c_df = pd.read_csv(c_file)
        ra_df = pd.read_csv(ra_file)

        f_symbols = f_df['Symbol'].unique()
        f_symbols = f_symbols + '-I'
        c_symbols = c_df['Symbol'].unique()
        ra_symbols = ra_df['Symbol'].unique()

        fset, cset, raset = set(f_symbols), set(c_symbols), set(ra_symbols)
        if len(list(fset - cset)) > 0:
            print(date, list(fset - cset), 'symbols not in continuous')
        if len(list(cset - raset)) > 0:
            print(date, list(cset - raset), 'symbols not in ratio adjusted')
        if len(list(cset - fset)) > 0:
            print(date, list(cset - fset), 'symbols extra in continuous')
        if len(list(raset - cset)) > 0:
            print(date, list(raset - cset), 'symbols extra in ratio adjusted')


def daily_symbol_check_v():
    """
    Test on volume/oi rollover considering no volume
    :return:
    """

    formatted_files = [f for f in os.listdir(FORMATTED) if f.endswith('.csv')]
    formatted_files.sort()


    for file in formatted_files:

        date = file[0:10]
        f_file = FORMATTED + file
        c_file = FORMATTED + VOL_CONTINUOUS + file
        ra_file = FORMATTED + VOL_CONTINUOUS + RATIO_ADJUSTED + file
        print(date, f_file, c_file, ra_file)

        f_df = pd.read_csv(f_file)
        c_df = pd.read_csv(c_file)
        ra_df = pd.read_csv(ra_file)

        f_df['Symbol'] = f_df['Symbol'].apply(str.strip)
        f_symbols = f_df['Symbol'].unique()
        f_vol_symbols = check_volume(f_df, f_df['Symbol'].unique())
        f_novol_symbols = list(set(f_symbols) - set(f_vol_symbols))
        c_symbols = c_df['Symbol'].unique()
        ra_symbols = ra_df['Symbol'].unique()

        print('f', f_symbols)
        print('f_vol', f_vol_symbols)
        print('f_novol', f_novol_symbols)
        print('c', c_symbols)
        print('ra', ra_symbols)


        fvset, fnvset, cset, raset =  set(f_vol_symbols), set(f_novol_symbols), set(c_symbols), set(ra_symbols)
        if len(list(fvset - cset)) > 0:
            print(date, list(fvset - cset), 'symbols not in continuous')
        if len(list(cset - raset)) > 0:
            print(date, list(cset - raset), 'symbols not in ratio adjusted')
        if len(list(cset - fvset)) > 0:
            pass
            #print(date, list(cset - fvset), 'symbols extra in continuous')
        if len(list(raset - cset)) > 0:
            pass
            #print(date, list(raset - cset), 'symbols extra in ratio adjusted')



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



def daily_signal_check_2():
    """
    Test on day rollover
    :return:
    """

    formatted_files = [f for f in os.listdir(FORMATTED) if f.endswith('.csv') and f[0:10] < '2005-02-01']
    formatted_files.sort()


    for file in formatted_files:

        date = file[0:10]
        f_file = FORMATTED + file
        c_file = FORMATTED + CONTINUOUS + file

        f_df = pd.read_csv(f_file)
        c_df = pd.read_csv(c_file)

        f_symbols = f_df['Symbol'].unique()
        c_symbols = c_df['Symbol'].unique()

        f1_symbols = []
        for symbol in f_symbols:
            f1_symbols.append(symbol + '-0')
            f1_symbols.append(symbol + '-I')
            f1_symbols.append(symbol + '-II')

        fset, f1set, cset = set(f_symbols), set(f1_symbols), set(c_symbols)
        if len(list(f1set - cset)) > 1:
            print(date, list(f1set - cset), 'symbols not in continuous')


def daily_symbol_check_formatted_continuous_only():
    """
    Test symbol selected or not
    :return:
    """

    formatted_files = [f for f in os.listdir(FORMATTED) if f.endswith('.csv')]
    formatted_files.sort()


    for file in formatted_files:

        date = file[0:10]
        f_file = FORMATTED + file

        c_file = FORMATTED + CONTINUOUS + FINAL + '-0/' + file
        #ra_file = FORMATTED + CONTINUOUS + RATIO_ADJUSTED + file

        f_df = pd.read_csv(f_file)
        c_df = pd.read_csv(c_file)
        #ra_df = pd.read_csv(ra_file)

        f_symbols = f_df['Symbol'].unique()
        #f_symbols = f_symbols + '-I'
        c_symbols = c_df['Symbol'].unique()
        #ra_symbols = ra_df['Symbol'].unique()

        #fset, cset, raset = set(f_symbols), set(c_symbols), set(ra_symbols)
        fset, cset = set(f_symbols), set(c_symbols)
        if len(list(fset - cset)) > 0:
            print(date, list(fset - cset), 'symbols not in continuous')
        #if len(list(cset - raset)) > 0:
        #    print(date, list(cset - raset), 'symbols not in ratio adjusted')
        if len(list(cset - fset)) > 0:
            print(date, list(cset - fset), 'symbols extra in continuous')
        #if len(list(raset - cset)) > 0:
        #    print(date, list(raset - cset), 'symbols extra in ratio adjusted')


os.chdir(PATH)
#os.chdir('test')
daily_symbol_check_formatted_continuous_only()



