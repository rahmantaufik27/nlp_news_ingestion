import argparse
import datetime
import os
import platform
import sys
import time

from web_scraping import ingest_wsj, ingest_wsj_archive, ingest_barrons, ingest_benzinga

parser = argparse.ArgumentParser()
parser.add_argument('--ingest_wsj', action='store_true', help='ingest wall street journal')
parser.add_argument('--ingest_wsj_archive', action='store_true', help='ingest archive wall street journal')
parser.add_argument('--ingest_barrons', action='store_true', help='ingest barrons news')
parser.add_argument('--ingest_benzinga', action='store_true', help='ingest benzinga news')
args = parser.parse_args()

if __name__ == '__main__':
    if args.ingest_wsj:
        ingest_wsj()
    elif args.ingest_wsj_archive:
        ingest_wsj_archive()
    elif args.ingest_barrons:
        ingest_barrons()
    elif args.ingest_benzinga:
        ingest_benzinga()
        
