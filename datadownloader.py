"""
Created on Feb 19, 2017
@author: Souvik
@Program Function: Download MCX bhavcopy data


"""

from selenium import webdriver
import time

url = 'https://www.mcxindia.com/market-data/bhavcopy'
chromedriver = 'C:/Users/Souvik/Downloads/chromedriver_win32/chromedriver.exe'

browser = webdriver.Chrome(chromedriver)

browser.get(url)

datepick = browser.find_element_by_id('txtDate')
print('###1', datepick)
datepick.click()

#date = browser.find_element_by_class_name('datepick-cmd datepick-cmd-prev')
#xpath = "//div[@class='datepick-month']//div[@class='datepick-ctrl]//a[@title='Clear all the dates'][text()='Clear']"
#xpath = "//div[@class='datepick-ctrl']/a[@title='Clear all the dates'][text()='Clear']"
#xpath = "//div[@class='datepick-ctrl']/a[@class='datepick-cmd datepick-cmd-clear ']"
#xpath = "//div[@class='datepick-month']/table/tbody/tr/td/a[@title='Select Wednesday, Feb 1, 2017'][text()='1']"
xpath = "//div[@class='datepick-month']/table/tbody/tr/td/a[@title='Select Wednesday, Feb 1, 2017']"
# xpath = "//div[@class='datepick-month']/table/tbody/tr/td/a[@title='Select Tuesday, Jan 31, 2017']"
date = browser.find_element_by_xpath(xpath)
print('###2', date)
time.sleep(1)
date.click()
print('############')
download = browser.find_element_by_id('cph_InnerContainerRight_C001_lnkExpToCSV')
print('###3', download)
download.click()

time.sleep(5)
browser.quit()

#driver.find_element_by_xpath("//div[@id='ui-datepicker-div']//a[@class='ui-state-default'][text()='HERE_IS_DATE_LIKE_10']")).click()
# meaning //div[@id='ui-datepicker-div']//td/a[@class='ui-state-default'][text()='10']

#"//div"\
#"//div[@class='datepick-month'[text()='13']"

#<a href="javascript:void(0)" title="Clear all the dates" class="datepick-cmd datepick-cmd-clear ">Clear</a>




