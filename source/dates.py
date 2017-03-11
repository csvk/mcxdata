"""
Created on Feb 21, 2017
@author: Souvik
@Program Function: Date utility functions


"""

from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import calendar

adhoc_dates = [] # Can be initiated with dates in YYYY-MM-DD format
yesterday = (date.today() - relativedelta(days=1)).strftime('%Y-%m-%d')
all_days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

def dates(start='2003-11-01', end=yesterday, days=all_days):
    """Return all dates between start and end"""

    dates = []

    curr_date = (datetime.strptime(start, '%Y-%m-%d') - relativedelta(days=1)).strftime('%Y-%m-%d')
    while curr_date < end:
        curr_date_strp = datetime.strptime(curr_date, '%Y-%m-%d') + relativedelta(days=1)
        curr_date = curr_date_strp.strftime('%Y-%m-%d')
        if calendar.day_name[curr_date_strp.weekday()] in days:
            dates.append(curr_date)

    #dates.append(curr_date)

    return dates

# Below functions take YYYY-MM-DD as input date format
def ddmmyy(date):
    return '{}{}{}'.format(date[8:10],date[5:7],date[2:4])


def ddmmyyyy(date):
    return '{}{}{}'.format(date[8:10],date[5:7],date[0:4])

def ddMMMyyyy(date):
    return '{}{}{}'.format(date[8:10],MMM(date),date[0:4])


def yyyy(date):
    return '{}'.format(date[0:4])


def MMM(date):
    return months(date[5:7], 'MMM')

def dayofweek(date):
    return calendar.day_name[datetime.strptime(date, '%Y-%m-%d').weekday()]

# Below functions take input as DDMMYY as input date format

def ddmmyy_to_yyyy_mm_dd(date): # Can handle dates years from 1961 to 2060
    return '{}{}-{}-{}'.format('19' if date[4:6] > '60' else '20', date[4:6], date[2:4], date[0:2])

def ddmmyyyy_to_yyyy_mm_dd(date): # Can handle dates years from 1961 to 2060
    return '{}-{}-{}'.format(date[4:8], date[2:4], date[0:2])

# Accepts month in numeric format of numeric in text format
def months(month, format='x'):
    """Return full month name"""

    month = int(month)
    if month < 0 or month > 12:
        return None
    month = str(month)

    months = {'01': 'January',
              '02': 'February',
              '03': 'March',
              '04': 'April',
              '05': 'May',
              '06': 'June',
              '07': 'July',
              '08': 'August',
              '09': 'September',
              '10': 'October',
              '11': 'November',
              '12': 'December',
              '1': 'January',
              '2': 'February',
              '3': 'March',
              '4': 'April',
              '5': 'May',
              '6': 'June',
              '7': 'July',
              '8': 'August',
              '9': 'September'
              }

    if format == 'x':
        return_val = months[month]
    elif format == 'Mmm':
        return_val = months[month][0:3]
    elif format == 'MMM':
        return_val = months[month][0:3].upper()
    elif format == 'mmm':
        return_val = months[month][0:3].lower()
    else:
        return_val = months[month]

    return return_val





