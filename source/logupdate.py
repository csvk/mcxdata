import os

location = 'C:/Users/Souvik/OneDrive/Python/mcxdata/data' # Laptop
logfile = 'log.txt'

os.chdir(location)

f = open(logfile, 'a')



f.write('\ntest data')
f.write('\ntest data 2')

l = ['\nxxxxxxx', '\nyyyyy']

f.writelines(l)

f.close()