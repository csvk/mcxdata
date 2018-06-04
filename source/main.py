
import csvhandler as ch
import datadbhandler as dbhandler
import os
#from pympler.tracker import SummaryTracker
import dates

PATH = 'D:/Trading/mcxdata/'


DBPATH = 'db/db.db'
CSVPATH = 'data_new/'
CSVDELTAPATH = 'delta/'




#tracker = SummaryTracker()

path = PATH
os.chdir(path)
#ch.ren_csv_files(path + CSVPATH)    # 1
#ch.format_csv_files(path + CSVPATH) # 2

db = dbhandler.DataDB(DBPATH)

#db.load_table_from_csv(path + CSVPATH) # 3

#db.process_staging_data() # 4
#db.append_continuous_contracts(start_date='2018-04-11')

#db.write_expiries() # 5
#db.create_continuous_contracts(['ALUMINIUM'], delta=0)
#db.create_continuous_contracts() # 6
#db.manage_missed_records(symbols=['ALUMINIUM', 'CRUDEOIL'])
#db.manage_missed_records() # 7
#db.update_continuous_contract() # 8
db.expiry_sanity_check() # 9
# db.calculate_historical_multipliers() # 10
# db.create_adjusted_contract() # 11


#tracker.print_diff()