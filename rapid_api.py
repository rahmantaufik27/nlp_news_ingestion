import requests
import json
import bs4
import pandas as pd
import os
import csv
import time
import sys
import urllib.request
from datetime import date
from htmldate import find_date
from dotenv import load_dotenv
from json import JSONDecoder

from fake_useragent import UserAgent
from user_agent import random_header

load_dotenv()


# INGESTION THROUGH RAPID API
def ingest_yahoo_finance():
    # this is code provided by rapid api
    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/get-news"

    # set query
    querystring = {"category":"TSLA","region":"US"}

    headers = {
        # use key from rapid api
        'x-rapidapi-key': os.getenv("rapidapi_key"),
        'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com"
        }

    response = requests.request("GET", url, headers=headers, params=querystring)
    # convert into json
    res = json.loads(response.text)

    length = len(res)
    print("total data news: ", length)

    # store to csv
    open_file = open('data_corpus/news/yahoo_finance.json', 'w')
    open_file.write(json.dumps(res, indent=4, sort_keys=True))
    open_file.close()

def ingest_seeking_alpha():
    url = "https://seeking-alpha.p.rapidapi.com/news/list"

    querystring = {"id":"aapl","size":"100","until":"0"}

    headers = {
        'x-rapidapi-key': os.getenv("rapidapi_key"),
        'x-rapidapi-host': "seeking-alpha.p.rapidapi.com"
        }

    response = requests.request("GET", url, headers=headers, params=querystring)
    res = json.loads(response.text)
  
    open_file = open('data_corpus/news/seeking_alpha_news.json', 'w')
    open_file.write(json.dumps(res, indent=4, sort_keys=True))
    open_file.close()

    url2 = "https://seeking-alpha.p.rapidapi.com/news/get-details"

    # looping id
    list_data = res['data']
    res2_all = []

    for list in list_data:
        print(list['id'])
        querystring2 = {"id":list['id']}

        headers2 = {
            'x-rapidapi-key': os.getenv("rapidapi_key"),
            'x-rapidapi-host': "seeking-alpha.p.rapidapi.com"
            }

        try:
            response2 = requests.request("GET", url2, headers=headers2, params=querystring2)
            res2 = json.loads(response2.text)
            res2_all.append(res2)
        except:
            continue

    length2 = len(res2_all)
    print("total data for news: ", length2)
  
    open_file2 = open('data_corpus/news/seeking_alpha_news_detail.json', 'w')
    open_file2.write(json.dumps(res2_all, indent=4, sort_keys=True))
    open_file2.close()

def ingest_seeking_alpha_article():
    url = "https://seeking-alpha.p.rapidapi.com/articles/list"

    querystring = {"filterCategory":"latest-articles","until":"0","size":"100"}

    headers = {
        'x-rapidapi-key': os.getenv("rapidapi_key"),
        'x-rapidapi-host': "seeking-alpha.p.rapidapi.com"
        }

    response = requests.request("GET", url, headers=headers, params=querystring)
    res = json.loads(response.text)

    length = len(res)
    print("total data for articles: ", length)
    
    open_file = open('data_corpus/news/seeking_alpha_article.json', 'w')
    open_file.write(json.dumps(res, indent=4, sort_keys=True))
    open_file.close()

    url2 = "https://seeking-alpha.p.rapidapi.com/articles/get-details"

    # example id
    querystring2 = {"id":"4395547"}

    headers2 = {
        'x-rapidapi-key': os.getenv("rapidapi_key"),
        'x-rapidapi-host': "seeking-alpha.p.rapidapi.com"
        }

    response2 = requests.request("GET", url2, headers=headers2, params=querystring2)
    res2 = json.loads(response2.text)

    length2 = len(res2)
    print("total data for articles detail: ", length2)
    
    open_file2 = open('data_corpus/news/seeking_alpha_article_detail.json', 'w')
    open_file2.write(json.dumps(res2, indent=4, sort_keys=True))
    open_file2.close()

def ingest_morningstar():

    def parse_object_pairs(pairs):
        return pairs

    url = "https://morning-star.p.rapidapi.com/news/list"

    querystring = {"performanceId":"0P0000OQN8"}

    headers = {
        'x-rapidapi-key': os.getenv("rapidapi_key"),
        'x-rapidapi-host': "morning-star.p.rapidapi.com"
        }

    response = requests.request("GET", url, headers=headers, params=querystring)
    res = json.loads(response.text)

    length = len(res)
    print("total data: ", length)
  
    open_file = open('data_corpus/news/morningstar_news.json', 'w')
    open_file.write(json.dumps(res, indent=4, sort_keys=True))
    open_file.close()

    url2 = "https://morning-star.p.rapidapi.com/news/get-details"


    res2_all = []
    for r in res:
        
        querystring2 = {"id":r['id'],"sourceId":r['sourceId']}

        headers2 = {
            'x-rapidapi-key': os.getenv("rapidapi_key"),
            'x-rapidapi-host': "morning-star.p.rapidapi.com"
            }

        try:
            response2 = requests.request("GET", url2, headers=headers2, params=querystring2)
            decoder = JSONDecoder(object_pairs_hook=parse_object_pairs)
            res2 = decoder.decode(response2)
            res2_all.append(res2)
        except:
            continue

    length2 = len(res2_all)
    print("total data detail news: ", length2)
  
    open_file2 = open('data_corpus/news/morningstar_news_detail.json', 'w')
    open_file2.write(json.dumps(res2_all, indent=4, sort_keys=True))
    open_file2.close()