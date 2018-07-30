#!/usr/bin/python
# coding:utf-8

from webFunctions import getAllPostsInMarket

def main():
    getAllPostsInMarket('https://www.tradingview.com/markets/cryptocurrencies/ideas/')

if __name__ == '__main__':
    main()