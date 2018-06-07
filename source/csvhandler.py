"""
Created on Mar 11, 2017
@author: Souvik
@Program Function: CSV handler

"""

import time, os
import dates, utils
import pandas as pd
import pickle as pkl
import csv
import selenium
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import traceback, logging

RENAMED = 'renamed/'
NODATA = 'nodata/'
FORMATTED = 'formatted/'

#LOCATION = 'D:/Trading/mcxdata/delta'
URL = 'https://www.mcxindia.com/market-data/bhavcopy'
CHROMEDRIVER = 'C:/Program Files (x86)/chromedriver_win32/chromedriver.exe'
LOGFILE = 'log.txt'


def download_bhavcopy(csvpath, start_date):
    
    utils.rmdir(csvpath)
    utils.mkdir(csvpath)
    #os.chdir(csvpath)

    log_lines = []

    # date_range = dates.dates('2017-02-17', '2017-02-20')
    date_range = dates.dates(start_date)
    # date_range = dates.adhoc_dates

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    prefs = {"download.default_directory": csvpath}
    options.add_experimental_option("prefs", prefs)
    # options.add_argument(LOCATION)

    browser = webdriver.Chrome(CHROMEDRIVER, chrome_options=options)
    browser.get(URL)

    select_year_xpath = "//div[@class='datepick-month-header']/select[@title='Change the year']"
    select_month_xpath = "//div[@class='datepick-month-header']/select[@title='Change the month']"
    no_data_xpath = "//*[@id='tblBhavCopy']/tbody/tr/td[text()='Data not available.']"

    try:
        for date in date_range:
            time.sleep(1)
            datepick = browser.find_element_by_id('txtDate')
            datepick.click()
            time.sleep(0.3)

            select_year = Select(browser.find_element_by_xpath(select_year_xpath))
            year, month = date[:4], dates.months(date[5:7])

            date_xpath = "//div[@class='datepick-month']/table/tbody/tr/td/a[text()='{}']".format(str(int(date[8:10])))
            select_year.select_by_visible_text(year)
            select_month = Select(browser.find_element_by_xpath(select_month_xpath))
            select_month.select_by_visible_text(month)
            select_date = browser.find_element_by_xpath(date_xpath)
            select_date.click()

            show = browser.find_element_by_id('btnShowDatewise')
            show.click()
            time.sleep(2)

            try:
                no_data = browser.find_element_by_xpath(no_data_xpath)
                log_line = 'No data for {}'.format(date)
                log_lines.append('\n{}'.format(log_line))
                print(log_line)
            except selenium.common.exceptions.NoSuchElementException:
                download = browser.find_element_by_id('cph_InnerContainerRight_C001_lnkExpToCSV')
                download.click()
                log_line = 'Downloading data for {}'.format(date)
                log_lines.append('\n{}'.format(log_line))
                print(log_line)
    except Exception as e:
        print('Program error, writing download log')
        logging.error(traceback.format_exc())

    f_log = open(csvpath + LOGFILE, 'a')
    f_log.writelines(log_lines)
    f_log.close()

    print('Download complete')

    time.sleep(20)

    browser.quit()


def ren_csv_files(path, csv_files_path, raw_bkp_path):

    csv_path = path + csv_files_path

    utils.mkdir(csv_path + RENAMED)
    utils.mkdir(csv_path + NODATA)

    csv_files = [f for f in os.listdir(csv_path) if f.endswith('.csv')]
    utils.copy_files(csv_path, raw_bkp_path, csv_files)

    print('Initiating renaming of {} files'.format(len(csv_files)))

    success, nodata, error = 0, 0, 0

    for file in csv_files:
        try:
            df = pd.read_csv(csv_path + file)
            if len(df.index) > 0:
                new_name = '{}{}.csv'.format(csv_path + RENAMED, dates.ddmmyyyy_to_yyyy_mm_dd(file[-12:][:8]))
                os.rename(csv_path + file, new_name)
                print(new_name, 'file renamed')
                success += 1
            else:
                new_name = '{}{}'.format(csv_path + NODATA, file)
                os.rename(csv_path + file, new_name)
                print(file, 'has no data')
                nodata += 1
        except:
            print(new_name, 'file rename failed')
            error += 1

    print('{} files renamed, {} files with no data, {} errors'.format(success, nodata, error))


def format_csv_files(path, csv_files_path):

    csv_path = path + csv_files_path

    utils.mkdir(csv_path + FORMATTED)

    csv_files = [f for f in os.listdir(csv_path + RENAMED) if f.endswith('.csv')]
    csv_files.sort()

    print('Initiating formatting of {} files'.format(len(csv_files)))
    print('Files range: {} - {}'.format(csv_files[0][0:10], csv_files[-1][0:10]))

    success, error = 0, 0

    for file in csv_files:
        try:
            date = file[0:10]  # Extract date from filename
            df = pd.read_csv(csv_path + RENAMED + file)
            df['Expiry Date'] = df['Expiry Date'].apply(dates.ddMMMyyyy_to_yyyy_mm_dd)  # Update Expiry Date Format
            if date <= '2017-03-03':
                df['Date'] = df['Date'].apply(dates.mm_dd_yyyy_to_yyyy_mm_dd)  # Update Date Format
            else:
                df['Date'] = df['Date'].apply(dates.dd_MMM_yyyy_to_yyyy_mm_dd) # Update Date Format
            df['Symbol'] = df['Symbol'].apply(str.strip)

            df.to_csv(csv_path + FORMATTED + file, sep=',', index=False)
            print(date, ',File formatted', file)
            success += 1
        except:
            print(date, ',Error in formatting', file)
            error += 1

    print('{} files formatted, {} errors'.format(success, error))

    return csv_files[0][0:10]









