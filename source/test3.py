
import csvhandler as ch
import os
import pickle as pkl

PATH = 'C:/Users/Souvik/OneDrive/Python/mcxdata/data - vol - oi rollover/' # Laptop volume rollover
#PATH = 'C:/Users/Souvik/OneDrive/Python/mcxdata/data - TDM rollover - 0-I/' # Laptop trading day rollover
FORMATTED = 'formatted/'
NODATA = 'nodata/'
NOTRADES = 'notrades.csv'
EXPIRIES = 'expiries.txt'
ROLLOVER_CLOSE = 'rollover_close.txt'
ROLLOVER_MULT = 'rollover_multipliers.txt'
CHANGED_DATES = 'changed_dates.txt'
CONTINUOUS = 'continuous/'
VOL_CONTINUOUS = 'continuous_vol/'
OI_CONTINUOUS = 'continuous_oi/'
RATIO_ADJUSTED = 'ratio_adjusted/'



#path = PATH + 'test2/'
path = PATH
os.chdir(path)
#ch.ren_csv_files()
os.chdir('test4/')
#os.chdir(FORMATTED)
os.chdir('continuous/final-0')

#ch.format_csv_futures('Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest', 'TDW', 'TDM', 'Expiry Date')
#ch.write_expiry_hist()
#ch.show_expiry_hist(True)
#ch.continuous_contracts_date_rollover(1)
#ch.continuous_contracts_vol_oi_rollover('Volume', debug=['CASTORSEED', 'CHANA', 'GOLDM', 'REFSOYOIL', 'URAD'])
#ch.continuous_contracts_vol_oi_rollover('Volume')
#os.chdir(VOL_CONTINUOUS)
#ch.ratio_adjust()
#os.chdir(RATIO_ADJUSTED)
#os.chdir('db')
#os.chdir(CONTINUOUS)
#ch.format_date('Date')
#os.chdir('test/')
#ch.continuous_contracts_select_days('I')

def show_changed_dates(rowwise=False):

    with open(CHANGED_DATES, 'rb') as handle:
        changed_dates = pkl.load(handle)

    print(changed_dates)

    if rowwise:
        for date, data in changed_dates.items():
            for symbol in data:
                print(date, symbol[0], symbol[1], symbol[2])


show_changed_dates(True)