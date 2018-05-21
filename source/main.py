
import csvhandler as ch
import datadbhandler as dbhandler
import os
#from pympler.tracker import SummaryTracker
import dates

PATH = 'D:/Trading/mcxdata/'


DBPATH = 'db/db.db'
CSVPATH = 'data/'
CSVDELTAPATH = 'delta/'




#tracker = SummaryTracker()

path = PATH
os.chdir(path)
#ch.ren_csv_files(path + CSVDELTAPATH)
#ch.format_csv_files(path + CSVDELTAPATH)

db = dbhandler.DataDB(DBPATH)

#db.load_table_from_csv(path + CSVDELTAPATH)

#db.process_staging_data()
#db.append_continuous_contracts(start_date='2018-04-11')

#db.write_expiries()
#db.create_continuous_contracts(['ALUMINIUM'], delta=0)
#db.create_continuous_contracts()
#db.manage_missed_records(symbols=['ALUMINIUM', 'CRUDEOIL'])
db.manage_missed_records()
#db.manage_missed_records(symbols=['ALUMINIUM'])
#db.manage_selected_records()
db.update_continuous_contract()
db.expiry_sanity_check()


#tracker.print_diff()