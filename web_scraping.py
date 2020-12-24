import requests
import json
import bs4
import pandas as pd
import os
import csv
import time
import sys
from datetime import date
from dotenv import load_dotenv
from bs4 import BeautifulSoup 
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from fake_useragent import UserAgent
from json import JSONDecoder
# from .user_agent import random_header

load_dotenv()

# INGESTION THROUGH WEB SCRAPING
def ingest_wsj():
    url = 'https://www.wsj.com/news/latest-headlines?mod=wsjheader'

    # call open browser function
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1920,1080")
    # chrome_options.headless = True
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    ua = UserAgent()
    userAgent = ua.random
    chrome_options.add_argument(f'user-agent={userAgent}')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    driver.get(url)

    sign_in_link = driver.find_element_by_link_text('Sign In')
    sign_in_link.click()

    username = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'username')))
    password = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'password')))
    username.send_keys(os.getenv('username_news'))
    password.send_keys(os.getenv('password_news'))

    driver.find_element_by_xpath("//*[@type='submit']").click()
    time.sleep(2)

    today = date.today()
    d1 = today.strftime("%d%m%Y")
    csv_file = open(f'data_news/wsj_articles_{d1}.csv', 'w', encoding='utf-8') 
    writer = csv.writer(csv_file)
 
    linklists = driver.find_elements_by_xpath('.//h2[@class="WSJTheme--headline--unZqjb45 reset WSJTheme--heading-3--2z_phq5h typography--serif-display--ZXeuhS5E "]//a[@href]')
    time.sleep(2)
    linklist_total = len(linklists)
    print("total news is: ", linklist_total)

    for i in range(0, linklist_total):
        article_dict = {}
        
        try:

            linklist = None
            linklist = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, './/h2[@class="WSJTheme--headline--unZqjb45 reset WSJTheme--heading-3--2z_phq5h typography--serif-display--ZXeuhS5E "]//a[@href]')))
            time.sleep(2)
            url_w = linklist[i].get_attribute('href')
            print(url_w)
            linklist[i].click()

            # url_w = url[i].get("href")

            try:
                headline1 = driver.find_element_by_xpath('.//h1[@class="wsj-article-headline"]')
                article_headline = headline1.text
            except:
                # if the tag is different with another page, try this one
                try:
                    headline1 = driver.find_element_by_xpath('.//h1[@class="bigTop__hed"]')
                    article_headline = headline1.text
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    article_headline = ''
                    print('pass title')
                    pass

            try:
                article_string = ''
                text1 = driver.find_elements_by_xpath(".//div[@class='article-content  ']//p")
                for ele in text1:
                    article_string += ele.text
                    # article_string = article_string.encode('utf-8')
            except (NoSuchElementException, StaleElementReferenceException) as e:
                article_string = ''
                print('pass text')
                pass

            try:
                time1 = driver.find_element_by_xpath(".//time[@class='timestamp article__timestamp flexbox__flex--1']")
                article_published_date = time1.text
            except (NoSuchElementException, StaleElementReferenceException) as e:
                article_published_date = ''
                print('pass date')
                pass

            
            article_dict['title'] = article_headline
            article_dict['news'] = article_string
            article_dict['created_at'] = article_published_date
            article_dict['publisher'] = 'Wall Street Journal'
            article_dict['link'] = url_w

            print("success scrap ", i+1)
        except Exception as e:
            print("failed scrap", e)
            time.sleep(1)
            pass

        writer.writerow(article_dict.values())
        driver.back()
        
    driver.quit()

def ingest_wsj_archive():
    url = 'https://www.wsj.com/news/archive/2020/march'

    # call open browser function
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.headless = True
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    driver.get(url)

    sign_in_link = driver.find_element_by_link_text('Sign In')
    sign_in_link.click()

    username = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'username')))
    password = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'password')))
    username.send_keys(os.getenv('username_news'))
    password.send_keys(os.getenv('password_news'))

    driver.find_element_by_xpath("//*[@type='submit']").click()
    time.sleep(2)

    csv_file = open('data_news/wsj_articles.csv', 'w', encoding='utf-8', newline='') 
    writer = csv.writer(csv_file)
    soup = BeautifulSoup(driver.page_source, 'lxml')
 
    daylinks = driver.find_elements_by_xpath('//a[@class="WSJTheme--day-link--19pByDpZ "][@href]')
    time.sleep(2)
    daylinks_total = len(daylinks)
    print("total link news is: ", daylinks_total)

    for i in range(20, daylinks_total):
        daylinks_click = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//a[@class="WSJTheme--day-link--19pByDpZ "][@href]')))
        time.sleep(2)
        daylinks_click[i].click()
        time.sleep(2)

        linklists = None
        linklists = driver.find_elements_by_xpath('.//h2[@class="WSJTheme--headline--unZqjb45 reset WSJTheme--heading-3--2z_phq5h typography--serif-display--ZXeuhS5E "]//a[@href]')
        time.sleep(2)
        linklist_total = len(linklists)
        print(f"total news on date {i+1} is : {linklist_total}")

        for i in range(0, linklist_total):
            article_dict = {}
            time.sleep(2)
            
            try:
                linklist = None
                linklist = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, './/h2[@class="WSJTheme--headline--unZqjb45 reset WSJTheme--heading-3--2z_phq5h typography--serif-display--ZXeuhS5E "]//a[@href]')))
                time.sleep(2)
                url_w = linklist[i].get_attribute('href')
                print(url_w)
                linklist[i].click()
                time.sleep(2)

                try:
                    headline1 = driver.find_element_by_xpath('.//h1[@class="wsj-article-headline"]')
                    article_headline = headline1.text
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    article_headline = ''
                    pass

                try:
                    article_string = ''
                    text1 = driver.find_elements_by_xpath(".//div[@class='article-content  ']//p")
                    for ele in text1:
                        article_string += ele.text
                        # article_string = article_string.encode('utf-8')
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    article_string = ''
                    pass

                try:
                    time1 = driver.find_element_by_xpath(".//time[@class='timestamp article__timestamp flexbox__flex--1']")
                    article_published_date = time1.text
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    article_published_date = ''
                    pass

                
                article_dict['title'] = str(article_headline)
                article_dict['news'] = str(article_string)
                article_dict['created_at'] = article_published_date
                article_dict['publisher'] = 'Wall Street Journal'
                article_dict['link'] = url_w

                print("success scrap ", i+1)
            except Exception as e:
                print("failed scrap", e)
                time.sleep(2)
                pass

            writer.writerow(article_dict.values())
            driver.back()
            time.sleep(2)

        driver.back()

    driver.quit()

def ingest_benzinga():
    url = 'https://www.benzinga.com/news'

    # call open browser function
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.headless = True
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    ua = UserAgent()
    userAgent = ua.random
    chrome_options.add_argument(f'user-agent={userAgent}')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    driver.get(url)

    sign_in_link = driver.find_element_by_link_text('Login')
    sign_in_link.click()

    username = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'edit-username-1')))
    password = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'edit-password-1')))
    username.send_keys("rahman.taufik@loratechai.com")
    password.send_keys("askLORA20$")

    button = driver.find_element_by_id('edit-submit-1')
    button.click()
    time.sleep(2)

    today = date.today()
    d1 = today.strftime("%d%m%Y")
    csv_file = open(f'data_news/benzinga_articles_{d1}.csv', 'w', encoding='utf-8') 
    writer = csv.writer(csv_file)
    article_dict = {}

    # collect news from 5 pages (actually the page can be set more than 5)
    for i in range(0, 5):
        linklists = driver.find_elements_by_xpath('.//span[@class="read-more"]//a[@href]')
        time.sleep(2)
        linklist_total = len(linklists)
        print("total news is: ", linklist_total)

        for i in range(0, linklist_total):
            try:
                time.sleep(1)
                linklist = None
                linklist = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, './/span[@class="read-more"]//a[@href]')))
                time.sleep(2)
                url_w = linklist[i].get_attribute('href')
                print(url_w)
                linklist[i].click()

                try:
                    headline1 = driver.find_element_by_xpath('.//h1[@id="title"]')
                    article_headline = headline1.text
                except:
                    # if the tag is different with another page, try this one
                    try:
                        headline1 = driver.find_element_by_xpath('.//h1[@class="bz3-article-page__title"]')
                        article_headline = headline1.text
                    except (NoSuchElementException, StaleElementReferenceException) as e:
                        article_headline = ''
                        print('pass title')
                        pass

                try:
                    article_string = ''
                    text1 = driver.find_elements_by_xpath(".//div[@class='article-content-body-only']//p")
                    for ele in text1:
                        article_string += ele.text
                        # article_string = article_string.encode('utf-8')
                except:
                    try:
                        article_string = ''
                        text1 = driver.find_elements_by_xpath(".//div[@id='article-body' and @class='bz3-article-page__content']//p")
                        for ele in text1:
                            article_string += ele.text
                        print(article_string)
                    except (NoSuchElementException, StaleElementReferenceException) as e:
                        article_string = ''
                        print('pass text')
                        pass

                try:
                    time1 = driver.find_element_by_xpath(".//span[@class='date']")
                    article_published_date = time1.text
                except:
                    try:
                        time1 = driver.find_element_by_xpath(".//div[@class='bz3-article-page__info']//span")
                        article_published_date = time1.text
                    except (NoSuchElementException, StaleElementReferenceException) as e:
                        article_published_date = ''
                        print('pass date')
                        pass
                
                article_dict['title'] = article_headline
                article_dict['news'] = article_string
                article_dict['created_at'] = article_published_date
                article_dict['publisher'] = 'Benzinga'
                article_dict['link'] = url_w

                print("success scrap ", i+1)
            except:
                print("failed scrap", e)
                time.sleep(1)
                pass

            writer.writerow(article_dict.values())
            driver.back()
        
        # Next Page
        try:
            sign_in_link = driver.find_element_by_link_text('next ›')
            time.sleep(1)
            sign_in_link.click()
        except:
            print("all page is done")
            pass
    
    driver.quit()

def ingest_barrons():
    url = 'https://www.barrons.com/real-time?mod=hp_LATEST&mod=hp_LATEST'

    # call open browser function
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.headless = True
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    driver.get(url)

    elem = driver.find_element_by_xpath('//a[@class="gdpr-close"]').click()
    time.sleep(2)
    sign_in_link = driver.find_element_by_link_text('Sign In')
    sign_in_link.click()

    username = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'username')))
    password = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'password')))
    username.send_keys(os.getenv('username_news'))
    password.send_keys(os.getenv('password_news'))

    driver.find_element_by_xpath("//*[@type='submit']").click()
    time.sleep(2)

    today = date.today()
    d1 = today.strftime("%d%m%Y")
    csv_file = open(f'data_news/barrons_articles_{d1}.csv', 'w', encoding='utf-8') 
    writer = csv.writer(csv_file)
    article_dict = {}
    
    # Collect all link list of latest news
    for i in range(0, 5):
        # Current Page
        linklists = driver.find_elements_by_xpath('.//h3[@class="BarronsTheme--lg--38o2HKJy BarronsTheme--RealTimeAnalysis--37l1n9tD BarronsTheme--headline--1OdorP8E BarronsTheme--heading-serif-1--2Qz5a5oa BarronsTheme--barrons-endmark--31VemKrx "]//a[@href]')
        time.sleep(2)
        linklist_total = len(linklists)
        print("Page ", i+1)
        print("total news is: ", linklist_total)

        for i in range(0, linklist_total):
            
            try:
                time.sleep(1)
                linklist = None
                linklist = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, './/h3[@class="BarronsTheme--lg--38o2HKJy BarronsTheme--RealTimeAnalysis--37l1n9tD BarronsTheme--headline--1OdorP8E BarronsTheme--heading-serif-1--2Qz5a5oa BarronsTheme--barrons-endmark--31VemKrx "]//a[@href]')))
                time.sleep(2)
                url_w = linklist[i].get_attribute('href')
                print(url_w)
                linklist[i].click()

                # url_w = url[i].get("href")

                try:
                    headline1 = driver.find_element_by_xpath('.//h1[@class="article__headline"]')
                    article_headline = headline1.text
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    article_headline = ''
                    print('pass title')
                    pass

                try:
                    article_string = ''
                    text1 = driver.find_elements_by_xpath(".//div[@class='article__body article-wrap at16-col16 barrons-article-wrap']//p")
                    for ele in text1:
                        article_string += ele.text
                        # article_string = article_string.encode('utf-8')
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    article_string = ''
                    print('pass text')
                    pass

                try:
                    time1 = driver.find_element_by_xpath(".//time[@class='timestamp article__timestamp flexbox__flex--1']")
                    article_published_date = time1.text
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    article_published_date = ''
                    print('pass date')
                    pass

                
                article_dict['title'] = article_headline
                article_dict['news'] = article_string
                article_dict['created_at'] = article_published_date
                article_dict['publisher'] = 'Barrons'
                article_dict['link'] = url_w

                print("success scrap ", i+1)
            except Exception as e:
                print("failed scrap", e)
                time.sleep(1)
                pass

            writer.writerow(article_dict.values())
            driver.back()

        # Next Page
        try:
            sign_in_link = driver.find_element_by_link_text('NEXT')
            time.sleep(1)
            sign_in_link.click()
        except:
            print("all page is done")
            pass

    driver.quit()