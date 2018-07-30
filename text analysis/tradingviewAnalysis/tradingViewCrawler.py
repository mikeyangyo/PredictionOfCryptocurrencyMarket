#!/usr/bin/python
# coding:utf-8

import argparse
import time

from typeChecking import dateChecking
from crawlerFunctions import getAllPostsInMarket
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='crawler for tradingview.com')

    # Optional argument
    parser.add_argument('-d',
                        '--date',
                        help='the require day, format should be yyyy-mm-dd',
                        type = dateChecking
                        )

    parser.add_argument('-n',
                        '--now',
                        help='update post data newer than newest data in database',
                        )

    args = vars(parser.parse_args())

    selectMarket = 'cryptocurrencies'

    start = time.time()

    if args['date'] is None and args['now'] is None:
        # get all post at specific market
        targetURL = 'https://www.tradingview.com/markets/{0}/ideas/'.format(selectMarket)
        getAllPostsInMarket(targetURL)
    elif args['date'] is not None and args['now'] is None:
        # get post on specific date at specific market
        selectDate = args['date']
        targetURL = 'https://www.tradingview.com/markets/{0}/ideas/'.format(selectMarket)
        getAllPostsInMarket(targetURL, selectDate)
    else:
        # check the post date of newest post in database
        # crawl all post after the date
        pass

if __name__ == '__main__':
    main()