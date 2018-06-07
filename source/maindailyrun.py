
import csvhandler as ch
import datadbhandler as dbhandler
import os
from pympler.tracker import SummaryTracker
import dates

PATH = 'D:/Trading/mcxdata/'

RAWBKPPATH = 'raw data/'
DBPATH = 'db/db.db'
CSVPATH = 'data/'
CSVDELTAPATH = 'delta/'
AMIBROKERPATH = 'amibroker/'


tracker = SummaryTracker()

path = PATH
os.chdir(path)

ch.download_bhavcopy(path + CSVDELTAPATH, '2018-06-06')

ch.ren_csv_files(path, CSVDELTAPATH, RAWBKPPATH)    # 1 : CSVPATH or CSVDELTAPATH
start_date = ch.format_csv_files(path, CSVDELTAPATH)  # 2 : CSVPATH or CSVDELTAPATH

db = dbhandler.DataDB(DBPATH)

db.load_table_from_csv(path + CSVDELTAPATH)  # 3 : CSVPATH or CSVDELTAPATH
db.process_staging_data()  # 4
db.write_expiries()  # 5

#db.create_continuous_contracts() # 6
db.append_continuous_contracts(start_date)  # 6.a

db.manage_missed_records()  # 7

db.update_continuous_contract()  # 8
db.expiry_sanity_check()  # 9
db.calculate_historical_multipliers()  # 10
db.create_adjusted_contract()  # 11

db.create_amibroker_import_files(AMIBROKERPATH, start_date) #12

tracker.print_diff()