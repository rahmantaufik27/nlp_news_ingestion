import argparse
import datetime
import os
import platform
import sys
import time

from web_scraping import ingest_wsj, ingest_wsj_archive, ingest_barrons, ingest_benzinga, ingest_google_news, ingest_wiki
from user_agent import list_test

parser = argparse.ArgumentParser()
parser.add_argument('--ingest_google_news', action='store_true', help='ingest google news based on tickers')
parser.add_argument('--ingest_wiki', action='store_true', help='ingest company description from wikipedia')
parser.add_argument('--ingest_wsj', action='store_true', help='ingest wall street journal')
parser.add_argument('--ingest_wsj_archive', action='store_true', help='ingest archive wall street journal')
parser.add_argument('--ingest_barrons', action='store_true', help='ingest barrons news')
parser.add_argument('--ingest_benzinga', action='store_true', help='ingest benzinga news')
parser.add_argument('--headers_list_test', action='store_true', help='test list of headers used for google scrap')
args = parser.parse_args()

if __name__ == '__main__':
    if args.ingest_google_news:
        ingest_google_news()
    elif args.ingest_wiki:
        ingest_wiki()
    elif args.ingest_wsj:
        ingest_wsj()
    elif args.ingest_wsj_archive:
        ingest_wsj_archive()
    elif args.ingest_barrons:
        ingest_barrons()
    elif args.ingest_benzinga:
        ingest_benzinga()
    
    if args.headers_list_test:
        list_test()
