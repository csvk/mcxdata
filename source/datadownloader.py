"""
Created on Feb 19, 2017
@author: Souvik
@Program Function: Download MCX bhavcopy data


"""

import selenium
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import time
import pandas

url = 'https://www.mcxindia.com/market-data/bhavcopy'
chromedriver = 'C:\Program Files (x86)/chromedriver_win32/chromedriver.exe'

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")

browser = webdriver.Chrome(chromedriver, chrome_options=options)

browser.get(url)

datepick = browser.find_element_by_id('txtDate')
print('###1', datepick)
datepick.click()




#date = browser.find_element_by_class_name('datepick-cmd datepick-cmd-prev')
#xpath = "//div[@class='datepick-month']//div[@class='datepick-ctrl]//a[@title='Clear all the dates'][text()='Clear']"
#xpath = "//div[@class='datepick-ctrl']/a[@title='Clear all the dates'][text()='Clear']"
#xpath = "//div[@class='datepick-ctrl']/a[@class='datepick-cmd datepick-cmd-clear ']"
#xpath = "//div[@class='datepick-month']/table/tbody/tr/td/a[@title='Select Wednesday, Feb 1, 2017'][text()='1']"
#xpath = "//div[@class='datepick-month']/table/tbody/tr/td/a[@title='Select Wednesday, Feb 1, 2017']"
dateText2 = 'Select Tuesday, Jan 31, 2017'
dateText = 'Select Thursday, Feb 16, 2017'
xpath = "//div[@class='datepick-month']/table/tbody/tr/td/a[@title='{}']".format(dateText)

date = None
try:
    date = browser.find_element_by_xpath(xpath)
except selenium.common.exceptions.NoSuchElementException:
    print('Date not found:', dateText)

print('###2', date)
show = browser.find_element_by_id('btnShowDatewise')

time.sleep(1)

#prev_month.click()
if date is not None:
    date.click()
    show.click()



print('###3')
download = browser.find_element_by_id('cph_InnerContainerRight_C001_lnkExpToCSV')
#except:
# print('a')
print('###4', download)
download.click()
time.sleep(2)

print('###4')

datepick.click()
xpath = "//div[@class='datepick-nav']/a[@title='Show the previous month']"
prev_month = browser.find_element_by_xpath(xpath)

select_year_xpath = "//div[@class='datepick-month-header']/select[@title='Change the year']"
select_month_xpath = "//div[@class='datepick-month-header']/select[@title='Change the month']"

select_year = Select(browser.find_element_by_xpath(select_year_xpath))
select_month = Select(browser.find_element_by_xpath(select_month_xpath))

select_year.select_by_visible_text('2016')
select_month.select_by_visible_text('August')

time.sleep(5)


print('###5')
date = None

#prev_month.click()
#prev_month = browser.find_element_by_xpath(xpath)
#prev_month.click()
#prev_month = browser.find_element_by_xpath(xpath)
#prev_month.click()
time.sleep(1)
print('###6')
xpath = "//div[@class='datepick-month']/table/tbody/tr/td/a[@title='{}']".format(dateText2)
try:
    date = browser.find_element_by_xpath(xpath)
except selenium.common.exceptions.NoSuchElementException:
    print('Date not found:', dateText2)
print('###7')
time.sleep(1)


if date is not None:
    date.click()
    show.click()

print('###8')
download.click()
time.sleep(2)
print('###9')
browser.quit()

#driver.find_element_by_xpath("//div[@id='ui-datepicker-div']//a[@class='ui-state-default'][text()='HERE_IS_DATE_LIKE_10']")).click()
# meaning //div[@id='ui-datepicker-div']//td/a[@class='ui-state-default'][text()='10']

#"//div"\
#"//div[@class='datepick-month'[text()='13']"

#<a href="javascript:void(0)" title="Clear all the dates" class="datepick-cmd datepick-cmd-clear ">Clear</a>

#<a href="javascript:void(0)" title="Show the previous month" class="datepick-cmd datepick-cmd-prev ">&lt;Prev</a>




