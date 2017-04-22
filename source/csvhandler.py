"""
Created on Mar 11, 2017
@author: Souvik
@Program Function: CSV handler


"""

import os
import dates, utils
import pandas as pd
import pickle as pkl

#PATH = 'C:/Users/Souvik/OneDrive/Python/mcxdata/data - vol - oi rollover/' # Laptop volume rollover
PATH = 'C:/Users/Souvik/OneDrive/Python/mcxdata/data - TDM rollover - 0-I/' # Laptop trading day rollover
FORMATTED = 'formatted/'
NODATA = 'nodata/'
NOTRADES = 'notrades.csv'
EXPIRIES = 'expiries.txt'
ROLLOVER_CLOSE = 'rollover_close.txt'
ROLLOVER_MULT = 'rollover_multipliers.txt'
NONE_SELECTED = 'none_selected.csv.txt'
FLAT_MULTIPLIER = 'flat_multiplier.csv.txt'
NXT_SELECTED = 'nxt_selected.csv.txt'
CONTINUOUS = 'continuous/'
SELECTED = 'selected/'
VOL_CONTINUOUS = 'continuous_vol/'
OI_CONTINUOUS = 'continuous_oi/'
MINEXP_CONTINUOUS = 'continuous_minexp/'
RATIO_ADJUSTED = 'ratio_adjusted/'

def format_csv_futures(*columns):

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()

    print('Initiating formatting of {} files'.format(len(csv_files)))

    cols = [c for c in columns]

    TDM, TDW = 0, 0
    save_month = 0
    success, error = 0, 0

    for file in csv_files:
        try:
            df = pd.read_csv(file)
            date = file[0:10]  # Extract date from filename
            df['Expiry Date'] = df['Expiry Date'].apply(dates.ddMMMyyyy_to_yyyy_mm_dd)  # Update Expiry Date Format
            df['Date'] = df['Date'].apply(dates.mm_dd_yyyy_to_yyyy_mm_dd) # Update Date Format
            df['Symbol'] = df['Symbol'].apply(str.strip)
            # Trading day of month
            if dates.mm_int(date) != save_month:  # New month
                save_month = dates.mm_int(date)
                TDM = 1
            else:
                TDM += 1
            # Trading day of week
            if dates.weekday(date) == 0:  # Monday
                TDW = 1
            else:
                TDW += 1
            df['TDM'] = [TDM] * len(df['Symbol'])
            df['TDW'] = [TDW] * len(df['Symbol'])
            df = df.reindex_axis(cols, axis=1)
            df.to_csv(file, sep=',', index=False)
            print(date, ',File formatted', file)
            success += 1
        except:
            print(date, ',Error in formatting', file)
            error += 1



    print('{} files formatted, {} errors'.format(success, error))


def ren_csv_files():

    utils.mkdir(FORMATTED)
    utils.mkdir(NODATA)

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]

    print('Initiating renaming of {} files'.format(len(csv_files)))

    success, nodata, error = 0, 0, 0

    for file in csv_files:
        try:
            df = pd.read_csv(file)
            if len(df.index) > 0:
                new_name = '{}{}.csv'.format(FORMATTED, dates.ddmmyyyy_to_yyyy_mm_dd(file[-12:][:8]))
                os.rename(file, new_name)
                print(new_name, 'file renamed')
                success += 1
            else:
                new_name = '{}{}'.format(NODATA, file)
                os.rename(file, new_name)
                print(file, 'has no data')
                nodata += 1
        except:
            print(new_name, 'file rename failed')
            error += 1

    print('{} files renamed, {} files with no data, {} errors'.format(success, nodata, error))



def write_expiry_hist(e_file=EXPIRIES):

    expiries = {}
    expiry_TDMs = {}

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]

    expiry_TDMs['1900-01-01'] = 0

    for file in csv_files:
        df = pd.read_csv(file)

        for index, row in df.iterrows():
            if row['Symbol'] not in expiries:
                expiries[row['Symbol']] = [row['Expiry Date']]
            if row['Expiry Date'] not in expiries[row['Symbol']]:
                expiries[row['Symbol']].append(row['Expiry Date'])
            if row['Expiry Date'] not in expiry_TDMs:
                expiry_TDMs[row['Expiry Date']] = 100 # Initialize
            if row['Expiry Date'] == row['Date'] and expiry_TDMs[row['Date']] == 100:
                expiry_TDMs[row['Expiry Date']] = row['TDM']

    for key, value in expiries.items():
        expiries[key].sort()

    max_date = max(csv_files)[0:10]

    for key, value in expiry_TDMs.items():
        fdate = key
        if value == 100 and fdate <= max_date:
            while(value == 100):
                if os.path.isfile('{}.csv'.format(fdate)):
                    df = pd.read_csv('{}.csv'.format(fdate))
                    expiry_TDMs[key], value = df['TDM'][0], df['TDM'][0]
                fdate = dates.relativedate(fdate, days=-1)

    with open(e_file, 'wb') as handle:
        pkl.dump({'expiry_dates': expiries, 'expiry_TDMs': expiry_TDMs}, handle)


def read_expiry_hist(e_file=EXPIRIES):

    with open(e_file, 'rb') as handle:
        expiry_hist = pkl.load(handle)

    return expiry_hist

def show_expiry_hist(rowwise=False):

    e = read_expiry_hist()
    print(e)

    if rowwise:
        for key, val in e['expiry_dates'].items():
            for v in val:
                print(key, v)

def trading_days_between(start, end, csv_files):
    return len([f[0:10] for f in csv_files if f[0:10] >= start and f[0:10] <= end])

def read_rollover_close_hist(e_file=ROLLOVER_CLOSE):

    with open(e_file, 'rb') as handle:
        rollover_close_hist = pkl.load(handle)

    return rollover_close_hist


def read_rollover_mult_hist(e_file=ROLLOVER_MULT):

    with open(e_file, 'rb') as handle:
        rollover_mult_hist = pkl.load(handle)

    return rollover_mult_hist


def dates_missing(start, end):

    csv_files = [f[0:10] for f in os.listdir(os.curdir) if f.endswith('.csv')]
    weekdays = dates.dates(start, end, ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'])

    missing_dates = list(set(weekdays) - set(csv_files))
    missing_dates.sort()

    return missing_dates


def rollover_multiplier_from_prev_dates(symbol, date, csv_files, prev_exp, curr_exp):

    reqd_files = [f for f in csv_files if f[0:10] < date]
    reqd_files.sort()

    multiplier = 1

    if prev_exp > '1900-01-01':
        for i in range(1, min(6, len(reqd_files) + 1)):
            df = pd.read_csv(reqd_files[-i])
            prev_record = df.loc[(df['Symbol'] == symbol) & (df['Expiry Date'] == prev_exp)]
            curr_record = df.loc[(df['Symbol'] == symbol) & (df['Expiry Date'] == curr_exp)]
            if not prev_record.empty and not curr_record.empty:
                multiplier = prev_record['Close'].iloc[0] / curr_record['Close'].iloc[0]
                break

    return multiplier


def select_expiry_new(csv_files, expiry_hist, date, symbol, delta):

    for expiry in expiry_hist['expiry_dates'][symbol]:
        if trading_days_between(date, expiry, csv_files) > delta and expiry > date:
            return expiry


def continuous_contracts_date_rollover_all(delta=None):
    """
    Create continuous contracts file with fixed rollover dates based on delta trading days from expiry
    :param delta: List of Contract rollover day differences from expiry day
    :return: None, Create continuous contracts file
    """

    if delta is None:
        delta = [0]

    if not os.path.isfile(EXPIRIES):
        write_expiry_hist()
    expiry_hist = read_expiry_hist(EXPIRIES)
    print(expiry_hist)

    utils.mkdir(CONTINUOUS)

    romans = {0: '0', 1: 'I', 2: 'II', 3: 'III', 4: 'IV', 5: 'V', 6: 'VI', 7: 'VII', 8: 'VIII', 9: 'IX',
              10: 'X', 11: 'XI',
              12: 'XII', 13: 'XIII', 14: 'XIV', 15: 'XV'}

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()
    print('Initiating continuous contract creation for {} days'.format(len(csv_files)))

    exp = [{}]
    success, error = 0, 0
    for file in csv_files:
        try:
            date = file[0:10]
            df = pd.read_csv(file)
            date_pd = pd.DataFrame()
            for symbol in df['Symbol'].unique():
                if symbol not in exp[0]:
                    for d in delta:
                        if d > 0:
                            exp.append({})
                        exp[d][symbol] = '1900-01-01'  # Initialize

                series = []
                for d in delta:
                    if expiry_hist['expiry_TDMs'][exp[d][symbol]] - df['TDM'][0] < d:
                        exp[d][symbol] = select_expiry_new(csv_files, expiry_hist, date, symbol, d)
                    series.append(df.loc[(df['Symbol'] == symbol) & (df['Expiry Date'] == exp[d][symbol])])
                    series[d]['Symbol'] = series[d]['Symbol'].apply(str.strip) + '-' + romans[d]
                    date_pd = pd.concat([date_pd, series[d]], axis=0)

            date_pd.to_csv('{}{}'.format(CONTINUOUS, file), sep=',', index=False)
            print(date, ',Continuous contract created', file)
            success += 1


        except:
            print(date, ',Error creating Continuous contract', file)
            error += 1

    print('Contract created for {} days, {} errors'.format(success, error))


def continuous_contracts_date_rollover(delta=None):
    """
    Create continuous contracts file with fixed rollover dates based on delta trading days from expiry
    :param delta: Contract rollover day difference from expiry day
    :return: None, Create continuous contracts file
    """

    if delta is None:
        delta = 0

    if not os.path.isfile(EXPIRIES):
        write_expiry_hist()
    expiry_hist = read_expiry_hist(EXPIRIES)
    print(expiry_hist)

    romans = {0: '0', 1: 'I', 2: 'II', 3: 'III', 4: 'IV', 5: 'V', 6: 'VI', 7: 'VII', 8: 'VIII', 9: 'IX',
              10: 'X', 11: 'XI',
              12: 'XII', 13: 'XIII', 14: 'XIV', 15: 'XV'}

    utils.mkdir(CONTINUOUS + str(delta))

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()
    print('Initiating continuous contract creation for {} days'.format(len(csv_files)))

    exp = {}
    rollover_multiplier = {}
    none_selected, flat_multiplier = pd.DataFrame(), pd.DataFrame()
    success, error = 0, 0
    for file in csv_files:
        try:
            date = file[0:10]
            df = pd.read_csv(file)
            date_pd = pd.DataFrame()
            for symbol in df['Symbol'].unique():
                if symbol not in exp:
                    exp[symbol] = '1900-01-01'  # Initialize
                    rollover_multiplier[symbol] = {}  # Intialize

                prev_exp = None
                if expiry_hist['expiry_TDMs'][exp[symbol]] - df['TDM'][0] < delta:
                    prev_exp = exp[symbol]
                    exp[symbol] = select_expiry_new(csv_files, expiry_hist, date, symbol, delta)
                    if exp[symbol] is None:
                        exp[symbol] = prev_exp
                    if prev_exp == exp[symbol]:
                        prev_exp = None

                sel_record = df.loc[(df['Symbol'] == symbol) & (df['Expiry Date'] == exp[symbol])]
                sel_record['Symbol'] = sel_record['Symbol'] + '-' + romans[delta]
                if sel_record.empty:
                    all_records = df.loc[df['Symbol'] == symbol]
                    if not all_records.empty:
                        all_records['Current Expiry'] = [exp[symbol] for rec in all_records.iterrows()]
                        none_selected = pd.concat([none_selected, all_records], axis=0)
                prev_record = pd.DataFrame()
                if prev_exp is not None:
                    prev_record = df.loc[(df['Symbol'] == symbol) & (df['Expiry Date'] == prev_exp)]
                    if prev_record.empty:
                        rollover_multiplier[symbol][date] = rollover_multiplier_from_prev_dates(symbol, date, csv_files,
                                                                                                prev_exp, exp[symbol])
                        if rollover_multiplier[symbol][date] == 1:
                            flat_multiplier = pd.concat(
                                [flat_multiplier, pd.DataFrame({'symbol': [symbol], 'date': [date]})], axis=0)

                if not sel_record.empty and not prev_record.empty:
                    rollover_multiplier[symbol][date] = prev_record['Close'].iloc[0] / \
                                                        sel_record['Close'].iloc[0]

                date_pd = pd.concat([date_pd, sel_record], axis=0)

            date_pd.to_csv('{}{}'.format(CONTINUOUS + str(delta) + '/', file), sep=',', index=False)
            print(date, ',Continuous contract created', file)
            success += 1

        except:
            print(date, ',Error creating Continuous contract', file)
            error += 1

    none_selected.to_csv('{}{}'.format(CONTINUOUS + str(delta) + '/', NONE_SELECTED), sep=',', index=False)
    flat_multiplier.to_csv('{}{}'.format(CONTINUOUS + str(delta) + '/', FLAT_MULTIPLIER), sep=',', index=False)

    with open(CONTINUOUS + str(delta) + '/' + ROLLOVER_MULT, 'wb') as handle:
        pkl.dump(rollover_multiplier, handle)

    print('Contract created for {} days, {} errors'.format(success, error))

    print(rollover_multiplier)


def continuous_contracts_select_days(delta):

    utils.mkdir(SELECTED)

    romans = {0: '0', 1: 'I', 2: 'II', 3: 'III', 4: 'IV', 5: 'V', 6: 'VI', 7: 'VII', 8: 'VIII', 9: 'IX',
              10: 'X', 11: 'XI',
              12: 'XII', 13: 'XIII', 14: 'XIV', 15: 'XV'}

    suffix = '-' + romans[delta]

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()

    success, error = 0, 0
    for file in csv_files:
        try:
            date = file[0:10]
            df = pd.read_csv(file)
            df_sel = df.loc[df['Symbol'].str[-len(suffix):] == suffix]
            df_sel.to_csv(SELECTED + file, sep=',', index=False)

            print(date, ', selected records for ', suffix, 'contract,', file)
            success += 1


        except:
            print(date, ',Error in selecting', file)
            error += 1

    print('Contract selected for {} days, {} errors'.format(success, error))


def continuous_contracts_vol_oi_rollover_record_prev_option(parm, record_next_exp=False):
    """
    Create continuous contracts file on volume or oi rollover
    :param parm: 'Volume' or 'Open Interest'
    :param record_next_exp: True if record for next expiry needs to be included on rollover day
    :return: None, create continuous contract files
    """

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()

    if parm == 'Volume':
        continuous = VOL_CONTINUOUS
    else:
        continuous = OI_CONTINUOUS

    utils.mkdir(continuous)

    if not os.path.isfile(EXPIRIES):
        write_expiry_hist()

    e = read_expiry_hist()
    print(e)

    exphist = e['expiry_dates']

    exp_idx = {}
    exp_rollover = {}
    success, error = 0, 0
    for file in csv_files:
        try:
            date = file[0:10]
            df = pd.read_csv(file)
            date_pd = pd.DataFrame()
            for symbol in df['Symbol'].unique():
                if symbol not in exp_idx:
                    exp_idx[symbol] = -1  # Initialize
                    exp_rollover[symbol] = False  # Initialize

                if exp_idx[symbol] == -1:
                    sel_record = df.loc[(df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][0])]
                    exp_idx[symbol] = 0
                elif exp_idx[symbol] == len(exphist[symbol]) - 1:
                    sel_record = df.loc[
                        (df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                else:
                    curr_record = df.loc[
                        (df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                    nxt_record = df.loc[(df['Symbol'] == symbol)
                                        & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol] + 1])]

                    if not curr_record.empty and not nxt_record.empty:
                        if exp_rollover[symbol]:
                            sel_record = curr_record
                            exp_rollover[symbol] = False
                        else:
                            sel_record = curr_record
                        if int(curr_record[parm]) < int(nxt_record[parm]):
                            exp_rollover[symbol] = True
                            exp_idx[symbol] += 1
                            if record_next_exp:
                                nxt_exp_record = df.loc[(df['Symbol'] == symbol)
                                                        & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                                nxt_exp_record['Symbol'] = nxt_exp_record['Symbol'].apply(str.strip) + '-N'
                                sel_record = pd.concat([sel_record, nxt_exp_record], axis=0)
                    elif curr_record.empty:
                        sel_record = nxt_record
                        exp_idx[symbol] += 1
                        if record_next_exp:
                            nxt_exp_record = df.loc[(df['Symbol'] == symbol)
                                                    & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                            nxt_exp_record['Symbol'] = nxt_exp_record['Symbol'].apply(str.strip) + '-N'
                            sel_record = pd.concat([sel_record, nxt_exp_record], axis=0)
                    elif nxt_record.empty:
                        sel_record = curr_record

                if not sel_record.empty:
                    date_pd = pd.concat([date_pd, sel_record], axis=0)

            date_pd['Symbol'] = date_pd['Symbol'].apply(str.strip)
            date_pd.to_csv('{}{}'.format(CONTINUOUS, file), sep=',', index=False)
            print(date, ',Continuous contract created', file)
            success += 1

        except:
            print(date, ',Error creating Continuous contract', file)
            error += 1

    print('Contract created for {} days, {} errors'.format(success, error))


def continuous_contracts_vol_oi_rollover_bkp(parm):
    """
    Create continuous contracts file on volume or oi rollover, create multipliers history
    :param parm: 'Volume' or 'Open Interest'
    :return: None, create continuous contract files
    """

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()

    if parm == 'Volume':
        path = VOL_CONTINUOUS
    elif parm == 'Open Interest':
        path = OI_CONTINUOUS

    utils.mkdir(path)

    if not os.path.isfile(EXPIRIES):
        write_expiry_hist()

    e = read_expiry_hist()
    print(e)

    exphist = e['expiry_dates']

    exp_idx = {}
    exp_rollover = {}
    rollover_multiplier = {}
    success, error = 0, 0
    for file in csv_files:
        try:
            date = file[0:10]
            df = pd.read_csv(file)
            date_pd = pd.DataFrame()
            for symbol in df['Symbol'].unique():
                if symbol not in exp_idx:
                    exp_idx[symbol] = -1  # Initialize
                    exp_rollover[symbol] = False  # Initialize
                    rollover_multiplier[str.strip(symbol)] = {}  # Initialize

                if exp_idx[symbol] == -1:  # First record for symbol
                    sel_record = df.loc[(df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][0])]
                    exp_idx[symbol] = 0
                elif exp_idx[symbol] == len(exphist[symbol]) - 1:  # Last expiry for symbol
                    sel_record = df.loc[
                        (df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                else:
                    curr_record = df.loc[
                        (df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                    nxt_record = df.loc[(df['Symbol'] == symbol)
                                        & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol] + 1])]

                    if not curr_record.empty and not nxt_record.empty:
                        if exp_rollover[symbol]:
                            sel_record = curr_record
                            exp_rollover[symbol] = False
                        else:
                            sel_record = curr_record
                        if int(curr_record[parm]) < int(nxt_record[parm]):
                            exp_rollover[symbol] = True
                            exp_idx[symbol] += 1
                            rollover_multiplier[str.strip(symbol)][date] = curr_record['Close'].iloc[0] / \
                                                                           nxt_record['Close'].iloc[0]
                    elif curr_record.empty and nxt_record.empty:
                        pass
                    elif curr_record.empty:
                        sel_record = nxt_record
                        exp_idx[symbol] += 1
                        rollover_multiplier[str.strip(symbol)][date] = 1
                    elif nxt_record.empty:
                        sel_record = curr_record

                if not sel_record.empty:
                    date_pd = pd.concat([date_pd, sel_record], axis=0)






            date_pd['Symbol'] = date_pd['Symbol'].apply(str.strip)
            date_pd.to_csv('{}{}'.format(path, file), sep=',', index=False)
            print(date, ',Continuous contract created', file)
            success += 1

        except:
            print(date, ',Error creating Continuous contract', file)
            error += 1

    with open(path + ROLLOVER_MULT, 'wb') as handle:
        pkl.dump(rollover_multiplier, handle)

    print('Contract created for {} days, {} errors'.format(success, error))


def continuous_contracts_vol_oi_rollover(parm):
    """
    Create continuous contracts file on volume or oi rollover, create multipliers history
    :param parm: 'Volume' or 'Open Interest'
    :return: None, create continuous contract files
    """

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()

    if parm == 'Volume':
        path = VOL_CONTINUOUS
    elif parm == 'Open Interest':
        path = OI_CONTINUOUS

    utils.mkdir(path)

    if not os.path.isfile(EXPIRIES):
        write_expiry_hist()

    e = read_expiry_hist()
    print(e)

    exphist = e['expiry_dates']

    exp_idx = {}
    exp_rollover = {}
    rollover_multiplier = {}
    none_selected, nxt_selected = pd.DataFrame(), pd.DataFrame()
    success, error = 0, 0
    for file in csv_files:
        try:
            date = file[0:10]
            df = pd.read_csv(file)
            date_pd = pd.DataFrame()
            for symbol in df['Symbol'].unique():
                curr_record, nxt_record, sel_record = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                if symbol not in exp_idx:
                    exp_idx[symbol] = -1  # Initialize
                    exp_rollover[symbol] = False  # Initialize
                    rollover_multiplier[str.strip(symbol)] = {}  # Initialize

                if exp_idx[symbol] == -1:  # First record for symbol
                    sel_record = df.loc[(df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][0])]
                    exp_idx[symbol] = 0
                elif exp_idx[symbol] == len(exphist[symbol]) - 1:  # Last expiry for symbol
                    sel_record = df.loc[
                        (df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                else:
                    curr_record = df.loc[
                        (df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                    nxt_record = df.loc[(df['Symbol'] == symbol)
                                        & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol] + 1])]
                    if not curr_record.empty and not nxt_record.empty:
                        if exp_rollover[symbol]:
                            sel_record = curr_record
                            exp_rollover[symbol] = False
                        else:
                            sel_record = curr_record
                        if int(curr_record[parm]) < int(nxt_record[parm]):
                            exp_rollover[symbol] = True
                            exp_idx[symbol] += 1
                            rollover_multiplier[str.strip(symbol)][date] = curr_record['Close'].iloc[0] / \
                                                                           nxt_record['Close'].iloc[0]
                    elif curr_record.empty and nxt_record.empty:
                        all_records = df.loc[df['Symbol'] == symbol]
                        if not all_records.empty:
                            none_selected = pd.concat([none_selected, all_records], axis=0)
                    elif curr_record.empty:
                        sel_record = nxt_record
                        exp_idx[symbol] += 1
                        nxt_selected = pd.concat([nxt_selected, nxt_record], axis=0)
                    elif nxt_record.empty:
                        sel_record = curr_record

                if not sel_record.empty:
                    date_pd = pd.concat([date_pd, sel_record], axis=0)

            date_pd.to_csv('{}{}'.format(path, file), sep=',', index=False)
            print(date, ',Continuous contract created', file)
            success += 1


        except:
            print(date, ',Error creating Continuous contract', file)
            error += 1

    none_selected.to_csv('{}{}'.format(path, NONE_SELECTED), sep=',', index=False)
    nxt_selected.to_csv('{}{}'.format(path, NXT_SELECTED), sep=',', index=False)

    with open(path + ROLLOVER_MULT, 'wb') as handle:
        pkl.dump(rollover_multiplier, handle)

    print('Contract created for {} days, {} errors'.format(success, error))


def continuous_contracts_vol_oi_rollover_debug(parm, debug=None):
    """
    Create continuous contracts file on volume or oi rollover, create multipliers history
    :param parm: 'Volume' or 'Open Interest'
    :return: None, create continuous contract files
    """

    if debug is None:
        debug = []

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()

    if parm == 'Volume':
        path = VOL_CONTINUOUS
    elif parm == 'Open Interest':
        path = OI_CONTINUOUS

    utils.mkdir(path)

    if not os.path.isfile(EXPIRIES):
        write_expiry_hist()

    e = read_expiry_hist()
    print(e)

    exphist = e['expiry_dates']

    exp_idx = {}
    exp_rollover = {}
    rollover_multiplier = {}
    success, error = 0, 0
    for file in csv_files:
        try:
            date = file[0:10]
            df = pd.read_csv(file)
            date_pd = pd.DataFrame()
            for symbol in df['Symbol'].unique():
                #print(symbol, debug)
                curr_record, nxt_record, sel_record = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                if symbol in debug:
                    print('@@@0', symbol, date)
                if symbol not in exp_idx:
                    exp_idx[symbol] = -1  # Initialize
                    exp_rollover[symbol] = False  # Initialize
                    rollover_multiplier[str.strip(symbol)] = {}  # Initialize

                if exp_idx[symbol] == -1:  # First record for symbol
                    sel_record = df.loc[(df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][0])]
                    exp_idx[symbol] = 0
                    if symbol in debug:
                        print('@@@1', symbol, date)
                elif exp_idx[symbol] == len(exphist[symbol]) - 1:  # Last expiry for symbol
                    sel_record = df.loc[
                        (df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                    if symbol in debug:
                        print('@@@2', symbol, date)
                else:
                    curr_record = df.loc[
                        (df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                    nxt_record = df.loc[(df['Symbol'] == symbol)
                                        & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol] + 1])]
                    if symbol in debug:
                        print('@@@3', symbol, date)

                    if not curr_record.empty and not nxt_record.empty:
                        if symbol in debug:
                            print('@@@4', symbol, date)
                        if exp_rollover[symbol]:
                            sel_record = curr_record
                            exp_rollover[symbol] = False
                            if symbol in debug:
                                print('@@@5', symbol, date)
                        else:
                            sel_record = curr_record
                            if symbol in debug:
                                print('@@@6', symbol, date)
                        if int(curr_record[parm]) < int(nxt_record[parm]):
                            exp_rollover[symbol] = True
                            exp_idx[symbol] += 1
                            rollover_multiplier[str.strip(symbol)][date] = curr_record['Close'].iloc[0] / \
                                                                           nxt_record['Close'].iloc[0]
                            if symbol in debug:
                                print('@@@7', symbol, date)
                    elif curr_record.empty and nxt_record.empty:
                        if symbol in debug:
                            print('@@@8', symbol, date, exphist[symbol][exp_idx[symbol]], exphist[symbol][exp_idx[symbol] + 1])
                        #pass

                    elif curr_record.empty:
                        sel_record = nxt_record
                        exp_idx[symbol] += 1
                        rollover_multiplier[str.strip(symbol)][date] = 1
                        if symbol in debug:
                            print('@@@9', symbol, date)
                    elif nxt_record.empty:
                        sel_record = curr_record
                        if symbol in debug:
                            print('@@@10', symbol, date)

                if not sel_record.empty:
                    date_pd = pd.concat([date_pd, sel_record], axis=0)






            date_pd['Symbol'] = date_pd['Symbol'].apply(str.strip)
            date_pd.to_csv('{}{}'.format(path, file), sep=',', index=False)
            print(date, ',Continuous contract created', file)
            success += 1

        except:
            print(date, ',Error creating Continuous contract', file)
            error += 1

    with open(path + ROLLOVER_MULT, 'wb') as handle:
        pkl.dump(rollover_multiplier, handle)

    print('Contract created for {} days, {} errors'.format(success, error))




def ratio_adjust():
    """
    Forward Ratio adjust continuous contract files
    :return: None, create forward ratio adjusted continuous contracts
    """

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()

    rollover_mult_hist = read_rollover_mult_hist()
    print(rollover_mult_hist)

    utils.mkdir(RATIO_ADJUSTED)

    multipliers, symbol_curr_close = {}, {}
    success, error = 0, 0
    for file in csv_files:
        try:
            date = file[0:10]
            df = pd.read_csv(file)

            for symbol in df['Symbol'].unique():
                if symbol not in multipliers:
                    multipliers[symbol] = 1  # Initialize

            for i, row in df.iterrows():
                df.ix[i, 'Open'] = round(df.ix[i, 'Open'] * multipliers[df.ix[i, 'Symbol']], 2)
                df.ix[i, 'High'] = round(df.ix[i, 'High'] * multipliers[df.ix[i, 'Symbol']], 2)
                df.ix[i, 'Low'] = round(df.ix[i, 'Low'] * multipliers[df.ix[i, 'Symbol']], 2)
                df.ix[i, 'Close'] = round(df.ix[i, 'Close'] * multipliers[df.ix[i, 'Symbol']], 2)

            for symbol in df['Symbol'].unique():
                if date in rollover_mult_hist[symbol]:
                    multipliers[symbol] = multipliers[symbol] * rollover_mult_hist[symbol][date]

            df.to_csv('{}{}'.format(RATIO_ADJUSTED, file), sep=',', index=False)
            print(date, ',Continuous contract created', file)
            success += 1



        except:
           print(date, ',Error creating Continuous contract', file)
           error += 1

    print('Contract created for {} days, {} errors'.format(success, error))


def ratio_adjust_same_day(delta):
    """
    Forward Ratio adjust continuous contract files
    :return: None, create forward ratio adjusted continuous contracts
    """

    romans = {0: '0', 1: 'I', 2: 'II', 3: 'III', 4: 'IV', 5: 'V', 6: 'VI', 7: 'VII', 8: 'VIII', 9: 'IX',
              10: 'X', 11: 'XI',
              12: 'XII', 13: 'XIII', 14: 'XIV', 15: 'XV'}

    os.chdir(str(delta))

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()

    rollover_mult_hist = read_rollover_mult_hist()
    print(rollover_mult_hist)

    utils.mkdir(RATIO_ADJUSTED)

    multipliers, symbol_curr_close = {}, {}
    success, error = 0, 0
    for file in csv_files:
        try:
            date = file[0:10]
            df = pd.read_csv(file)

            df['Symbol'] = df['Symbol'].str[:-(len(romans[delta]) + 1)]

            for symbol in df['Symbol'].unique():
                if symbol not in multipliers:
                    multipliers[symbol] = 1  # Initialize
                if date in rollover_mult_hist[symbol]:
                    multipliers[symbol] = multipliers[symbol] * rollover_mult_hist[symbol][date]

            for i, row in df.iterrows():
                df.ix[i, 'Open'] = round(df.ix[i, 'Open'] * multipliers[df.ix[i, 'Symbol']], 2)
                df.ix[i, 'High'] = round(df.ix[i, 'High'] * multipliers[df.ix[i, 'Symbol']], 2)
                df.ix[i, 'Low'] = round(df.ix[i, 'Low'] * multipliers[df.ix[i, 'Symbol']], 2)
                df.ix[i, 'Close'] = round(df.ix[i, 'Close'] * multipliers[df.ix[i, 'Symbol']], 2)

            df['Symbol'] = df['Symbol'] + '-' + romans[delta]

            df.to_csv('{}{}'.format(RATIO_ADJUSTED, file), sep=',', index=False)
            print(date, ',Ratio adjusted Continuous contract created', file)
            success += 1




        except:
           print(date, ',Error creating Continuous contract', file)
           error += 1

    print('Ratio  adjusted Contract created for {} days, {} errors'.format(success, error))


def format_date(*kwargs):

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]

    success, error = 0, 0
    for file in csv_files:
        try:
            date = file[0:10]
            df = pd.read_csv(file)
            for col in kwargs:
                df[col] = df[col].apply(dates.mm_dd_yyyy_to_yyyy_mm_dd)
            df.to_csv(file, sep=',', index=False)

            print(date, ',date formatted', file)
            success += 1

        except:
            print(date, ',Error in formatting', file)
            error += 1

    print('Contract formatted for {} days, {} errors'.format(success, error))














