
import csvhandler as ch
import os
from pympler.tracker import SummaryTracker

PATH = 'C:/Users/Souvik/OneDrive/Python/mcxdata/data - vol - oi rollover/' # Laptop volume rollover
#PATH = 'C:/Users/Souvik/OneDrive/Python/mcxdata/data - TDM rollover - 0-I/' # Laptop trading day rollover
FORMATTED = 'formatted/'
NODATA = 'nodata/'
NOTRADES = 'notrades.csv'
EXPIRIES = 'expiries.txt'
ROLLOVER_CLOSE = 'rollover_close.txt'
ROLLOVER_MULT = 'rollover_multipliers.txt'
CONTINUOUS = 'continuous/'
INTERMEDIATE = 'intermediate'
FINAL = 'final'
VOL_CONTINUOUS = 'continuous_vol/'
OI_CONTINUOUS = 'continuous_oi/'
RATIO_ADJUSTED = 'ratio_adjusted/'


tracker = SummaryTracker()

#path = PATH + 'test2/'
path = PATH
os.chdir(path)
#ch.ren_csv_files()
#os.chdir('test4/')
os.chdir(FORMATTED)

#ch.format_csv_futures('Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest', 'TDW', 'TDM', 'Expiry Date')
#ch.write_expiry_hist()
#ch.show_expiry_hist(False)
#ch.continuous_contracts_all([0,1,2])
#ch.continuous_contracts_vol_oi_rollover('Volume', debug=['CASTORSEED', 'CHANA', 'GOLDM', 'REFSOYOIL', 'URAD'])
#ch.continuous_contracts_vol_oi_rollover_debug('Volume', ['BRCRUDEOIL', 'GUARGUM', 'MAIZE', 'REFSOYOIL', 'RUBBER'])
#ch.continuous_contracts_vol_oi_rollover('Volume')
#ch.continuous_contracts_date_rollover()
#ch.continuous_contracts_date_rollover_debug(1)
#ch.continuous_contracts_date_rollover_vol_rollover_fix()
#ch.calc_rollover_multipliers()
#print(ch.read_rollover_mult_hist('{}/{}'.format(CONTINUOUS + INTERMEDIATE + '-' + '0', ROLLOVER_MULT)))
#ch.show_rollover_mult_hist('{}/{}'.format(CONTINUOUS + FINAL + '-' + '0', ROLLOVER_MULT))
os.chdir(CONTINUOUS)
os.chdir(FINAL + '-0')
ch.ratio_adjust_same_day()
#os.chdir(RATIO_ADJUSTED)
#os.chdir('db')
#os.chdir(CONTINUOUS)
#ch.format_date('Date')
#os.chdir('test/')
#ch.continuous_contracts_select_days('I')
#os.chdir(CONTINUOUS)
#ch.ratio_adjust_same_day()



tracker.print_diff()