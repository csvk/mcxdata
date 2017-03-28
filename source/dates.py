"""
Created on Feb 21, 2017
@author: Souvik
@Program Function: Date utility functions


"""

from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import calendar

#adhoc_dates = [] # Can be initiated with dates in YYYY-MM-DD format
yesterday = (date.today() - relativedelta(days=1)).strftime('%Y-%m-%d')
all_days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

def dates(start='2008-06-01', end=yesterday, days=all_days):
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

def mm_int(date):
    """
    :param date:
    :return: Month in integer format
    """
    return int(date[5:7])

def ddmmyyyy(date):
    return '{}{}{}'.format(date[8:10],date[5:7],date[0:4])

def ddMMMyyyy(date):
    return '{}{}{}'.format(date[8:10],MMM(date),date[0:4])


def yyyy(date):
    return '{}'.format(date[0:4])


def MMM(date):
    return months(date[5:7], 'MMM')

def weekday(date):
    return datetime.strptime(date, '%Y-%m-%d').weekday()

def dayofweek(date):
    return calendar.day_name[weekday(date)]

def relativedate(date, years=0, months=0, days=0):
    return (datetime.strptime(date, '%Y-%m-%d')
            + relativedelta(days=days, months=months, years=years)).strftime('%Y-%m-%d')

def setdate(date, year=0, month=0, day=0):

    strpdate = datetime.strptime(date, '%Y-%m-%d')

    if year > 0:
        strpdate = strpdate.replace(year=year)
    if month > 0:
        strpdate = strpdate.replace(month=month)
    if day > 0:
        strpdate = strpdate.replace(day=day)

    return strpdate.strftime('%Y-%m-%d')

def datediff(date1, date2):
    return (datetime.strptime(date1, '%Y-%m-%d') - datetime.strptime(date2, '%Y-%m-%d')).days

# Below functions take input as DDMMYY as input date format

def ddmmyy_to_yyyy_mm_dd(date): # Can handle dates years from 1961 to 2060
    return '{}{}-{}-{}'.format('19' if date[4:6] > '60' else '20', date[4:6], date[2:4], date[0:2])

# Below functions take input as DDMMMYY as input date format

def ddMMMyyyy_to_yyyy_mm_dd(date):
    return '{}-{}-{}'.format(date[5:9], mm(date[2:5]), date[0:2])

# Below functions take input as DD-MMM-YY as input date format

def dd_MMM_yyyy_to_yyyy_mm_dd(date):
    return '{}-{}-{}'.format(date[7:11], mm(date[3:6]), date[0:2])

def ddmmyyyy_to_yyyy_mm_dd(date):
    return '{}-{}-{}'.format(date[4:8], date[2:4], date[0:2])

# Below functions take Month name as input: full name of first three chars

def mm(month):
    """Return month in MM format"""

    months = {'JAN': '01',
              'FEB': '02',
              'MAR': '03',
              'APR': '04',
              'MAY': '05',
              'JUN': '06',
              'JUL': '07',
              'AUG': '08',
              'SEP': '09',
              'OCT': '10',
              'NOV': '11',
              'DEC': '12',
              'JANUARY': '01',
              'FEBRUARY': '02',
              'MARCH': '03',
              'APRIL': '04',
              'JUNE': '06',
              'JULY': '07',
              'AUGUST': '08',
              'SEPTEMBER': '09',
              'OCTOBER': '10',
              'NOVEMBER': '11',
              'DECEMBER': '12'
              }

    month = month.upper()

    if  month in months:
        return months[month]
    else:
        return None


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


adhoc_dates = ['2003-11-17', '2003-11-26', '2003-12-25', '2004-01-26', '2004-02-02', '2004-03-02', '2004-04-09',
               '2004-11-12', '2004-11-15', '2005-01-26', '2005-03-25', '2005-07-28', '2005-08-15', '2005-12-16',
               '2006-01-26', '2006-04-14', '2006-08-15', '2006-10-02', '2006-12-25', '2007-01-26', '2007-08-15',
               '2007-10-02', '2007-12-25', '2008-03-21', '2008-08-15', '2008-10-02', '2008-11-27', '2008-12-25',
               '2009-01-26', '2009-04-10', '2009-04-30', '2009-10-02', '2009-12-25', '2010-01-26', '2010-04-02',
               '2011-01-26', '2011-04-22', '2011-08-15', '2012-01-26', '2012-04-06', '2012-08-15', '2012-10-02',
               '2012-12-25', '2013-03-29', '2013-08-15', '2013-10-02', '2013-12-25', '2014-04-18', '2014-04-24',
               '2014-08-15', '2014-10-02', '2014-10-15', '2014-12-25', '2015-01-26', '2015-04-03', '2015-10-02',
               '2015-12-25', '2016-01-26', '2016-03-25', '2016-08-15', '2017-01-26']


