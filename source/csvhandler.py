"""
Created on Mar 11, 2017
@author: Souvik
@Program Function: CSV handler


"""

import os
import dates, utils
import pandas as pd

PATH = 'C:/Users/Souvik/OneDrive/Python/mcxdata/data new/' # Laptop
FORMATTED = 'formatted/'

def format_csv_futures(path, *columns):

    os.chdir(path)
    utils.mkdir(FORMATTED)

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]

    print('Initiating formatting of {} files'.format(len(csv_files)))

    cols = [c for c in columns]

    for file in csv_files:
        try:
            df = pd.read_csv(file)
            date = dates.ddmmyyyy_to_yyyy_mm_dd(file[-12:][:8]) # Extract date from filename
            df['Expiry Date'] = df['Expiry Date'].apply(dates.ddMMMyyyy_to_yyyy_mm_dd)  # Update Expiry Date Format
            df = df.reindex_axis(cols, axis=1)
            df.to_csv('{}{}'.format(FORMATTED, file), sep=',', index=False)
            print(date, ',File formatted', file)
        except:
            print(date, ',Error in formatting', file)


def ren_csv_files(path):

    os.chdir(path)

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]

    for file in csv_files:
        try:
            #new_name = '{}.csv'.format(dates.ddmmyy_to_yyyy_mm_dd(file[-10:][:8]))
            new_name = '{}.csv'.format(dates.ddmmyyyy_to_yyyy_mm_dd(file[-12:][:8]))
            os.rename(file, new_name)
            print(new_name, 'file renamed')
        except:
            print(new_name, 'file rename failed')

#path = '{}{}'.format(PATH, 'test/')
path = PATH
os.chdir(path)
format_csv_futures(path,
                   'Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest', 'Expiry Date')
ren_csv_files(FORMATTED)
