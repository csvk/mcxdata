"""
Created on Mar 11, 2017
@author: Souvik
@Program Function: CSV handler

"""

import os
import dates, utils
import pandas as pd
import pickle as pkl
import csv


RENAMED = 'renamed/'
NODATA = 'nodata/'
FORMATTED = 'formatted/'


def ren_csv_files(csv_path):

    utils.mkdir(csv_path + RENAMED)
    utils.mkdir(csv_path + NODATA)

    csv_files = [f for f in os.listdir(csv_path) if f.endswith('.csv')]

    print('Initiating renaming of {} files'.format(len(csv_files)))

    success, nodata, error = 0, 0, 0

    for file in csv_files:
        try:
            df = pd.read_csv(csv_path + file)
            if len(df.index) > 0:
                new_name = '{}{}.csv'.format(csv_path + RENAMED, dates.ddmmyyyy_to_yyyy_mm_dd(file[-12:][:8]))
                os.rename(csv_path + file, new_name)
                print(new_name, 'file renamed')
                success += 1
            else:
                new_name = '{}{}'.format(csv_path + NODATA, file)
                os.rename(csv_path + file, new_name)
                print(file, 'has no data')
                nodata += 1
        except:
            print(new_name, 'file rename failed')
            error += 1

    print('{} files renamed, {} files with no data, {} errors'.format(success, nodata, error))


def format_csv_files(csv_path):

    utils.mkdir(csv_path + FORMATTED)

    csv_files = [f for f in os.listdir(csv_path + RENAMED) if f.endswith('.csv')]
    csv_files.sort()

    print('Initiating formatting of {} files'.format(len(csv_files)))

    success, error = 0, 0

    for file in csv_files:
        try:
            date = file[0:10]  # Extract date from filename
            df = pd.read_csv(csv_path + RENAMED + file)
            df['Expiry Date'] = df['Expiry Date'].apply(dates.ddMMMyyyy_to_yyyy_mm_dd)  # Update Expiry Date Format
            if date <= '2017-03-03':
                df['Date'] = df['Date'].apply(dates.mm_dd_yyyy_to_yyyy_mm_dd)  # Update Date Format
            else:
                df['Date'] = df['Date'].apply(dates.dd_MMM_yyyy_to_yyyy_mm_dd) # Update Date Format
            df['Symbol'] = df['Symbol'].apply(str.strip)

            df.to_csv(csv_path + FORMATTED + file, sep=',', index=False)
            print(date, ',File formatted', file)
            success += 1
        except:
            print(date, ',Error in formatting', file)
            error += 1

    print('{} files formatted, {} errors'.format(success, error))









