
import csvhandler as ch
import datadbhandler as dbhandler
import os
#from pympler.tracker import SummaryTracker
import dates

PATH = 'D:/Trading/mcxdata/'

DBPATH = 'db/db.db'



#tracker = SummaryTracker()

path = PATH
os.chdir(path)
#ch.ren_csv_files()

db = dbhandler.DataDB(DBPATH)

#symbols = db.unique_symbols()
#print(symbols)

#db.dump_record_count()
#db.select_symbol_records('COTTON')
#db.write_expiries()
#db.create_continuous_contracts(['ALUMINIUM'], delta=0)
#db.create_continuous_contracts()
#db.manage_missed_records(symbols=['ALUMINIUM', 'CRUDEOIL'])
#db.manage_missed_records()
#db.manage_missed_records_2()
#db.manage_missed_records_2(symbols=['ALUMINIUM'])
#db.manage_missed_records_3(symbols=['BRCRUDEOIL'])
#db.manage_selected_records()
#db.update_continuous_contract()
#db.test_uncommitted_read()
db.expiry_sanity_check()

#d = dates.dates('2018-04-06', '2018-04-10', ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'])
#print(len(d))

#tracker.print_diff()