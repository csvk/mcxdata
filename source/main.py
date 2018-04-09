
import csvhandler as ch
import csvtodb as c2db
import os
# from pympler.tracker import SummaryTracker
import dates

PATH = 'D:/Trading/mcxdata/'

DBPATH = 'db/db.db'



# tracker = SummaryTracker()

path = PATH
os.chdir(path)
#ch.ren_csv_files()

db = c2db.DataDB(DBPATH)

#db.test()
#symbols = db.unique_symbols()
#print(symbols)

#db.select_symbol_records('COTTON')
#db.write_expiries()
db.create_continuous_contracts(['ALUMINI'])



#tracker.print_diff()