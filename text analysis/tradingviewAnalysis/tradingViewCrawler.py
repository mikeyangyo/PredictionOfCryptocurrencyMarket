#!/usr/bin/python
# coding:utf-8

import argparse
import time

from typeChecking import dateChecking
from typeChecking import marketChecking
from crawlerFunctions import getAllPostsInMarket
from crawlerFunctions import getMarkets
from datetime import datetime
def main():
    parser = argparse.ArgumentParser(description='crawler for tradingview.com')

    markets = getMarkets()
    # Positional argument

    parser.add_argument('market',
                        help='/'.join(markets),
                        type = marketChecking
                        )

    # Optional argument
    parser.add_argument('-d',
                        '--date',
                        help='the require day, format should be yyyy-mm-dd',
                        type = dateChecking
                        )

    args = vars(parser.parse_args())

    selectMarket = args['market']

    start = time.time()

    if args['date'] is None:
        # get all post at specific market
        targetURL = 'https://www.tradingview.com/markets/{0}/ideas/'.format(selectMarket)
        getAllPostsInMarket(targetURL)
    else:
        # get post on specific date at specific market
        selectDate = args['date']
        targetURL = 'https://www.tradingview.com/markets/{0}/ideas/'.format(selectMarket)
        getAllPostsInMarket(targetURL, selectDate)

if __name__ == '__main__':
    main()