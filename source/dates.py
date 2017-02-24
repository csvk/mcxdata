"""
Created on Feb 21, 2017
@author: Souvik
@Program Function: Dates list along with click requirements


"""

from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import calendar

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

def months(month):
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

    return months[month]





