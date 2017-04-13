
import csvhandler as ch
import os

PATH = 'C:/Users/Souvik/OneDrive/Python/mcxdata/data - vol - oi rollover/' # Laptop
FORMATTED = 'formatted/'
NODATA = 'nodata/'
NOTRADES = 'notrades.csv'
EXPIRIES = 'expiries.txt'
ROLLOVER_CLOSE = 'rollover_close.txt'
ROLLOVER_MULT = 'rollover_multipliers.txt'
CONTINUOUS = 'continuous/'
VOL_CONTINUOUS = 'continuous_vol/'
OI_CONTINUOUS = 'continuous_oi/'
RATIO_ADJUSTED = 'ratio_adjusted/'



path = PATH
os.chdir(path)
ch.ren_csv_files()
os.chdir(FORMATTED)
ch.format_csv_futures('Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest', 'TDW', 'TDM', 'Expiry Date')
#ch.write_expiry_hist()
#ch.show_expiry_list(False)
#ch.continuous_contracts_all([0,1,2,3,4,5,6,7,8,9,10])
continuous_contracts_vol_oi_rollover('Volume')
os.chdir(VOL_CONTINUOUS)
ch.ratio_adjust()
#os.chdir(RATIO_ADJUSTED)
#os.chdir('db')
#ch.format_date('DATE')
