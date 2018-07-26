#!/usr/bin/python
#coding:utf-8

import requests
import datetime
import string
from bs4 import BeautifulSoup

def main():
    getAllPostsInMarket('https://www.tradingview.com/markets/cryptocurrencies/ideas/')

def getAllPostsInMarket(startURL):
    curURL = startURL
    curPage = 1
    allResult = []

    totalPage = getTotalPageNumber(startURL)
    print 'processing page 1...'
    result = getAllPostsInPage(curURL)
    allResult.append(result)

    for i in range(2, totalPage + 1):
        pagePostfix = 'page-' + str(i)
        print 'processing ', pagePostfix, '...'
        absoluteNextPageURL = requests.compat.urljoin(startURL, pagePostfix)
        curURL = absoluteNextPageURL

        result = getAllPostsInPage(curURL)
        if result in allResult:
            print 'something error occured, the result is duplicated'
            break
        
        allResult.append(result)

def getAllPostsInPage(startURL):    
    # download content of cryptocurrencies at tradingview.com
    r = requests.get(startURL)

    # check if the content is valid or not
    if r.status_code != requests.codes.ok:
        return None
    
    # use bs4 to reveal the content with html
    soup = BeautifulSoup(r.text, 'html.parser')

    #find idea title by css class
    ideaURLs = soup.select('div.tv-site-widget.tv-widget-idea.js-widget-idea > div.tv-site-widget__body.tv-widget-idea.js-feed__item-minimizable-area.js-widget-body > a.tv-widget-idea__title.js-widget-idea__popup')

    URLs = []
    for ideaURL in ideaURLs:
        url = ideaURL.get('href')
        absoluteURL = requests.compat.urljoin(startURL, url)
        #URLs.append(absoluteURL)
        print getPostInfo(absoluteURL)

    return URLs

def getTotalPageNumber(startURL):
    # download content of cryptocurrencies at tradingview.com
    r = requests.get(startURL)

    # check if the content is valid or not
    if r.status_code != requests.codes.ok:
        return None
    
    # use bs4 to reveal the content with html
    soup = BeautifulSoup(r.text, 'html.parser')

    # find page urls by css class
    PageURLs = soup.select('a.tv-load-more__page')
    lastPageURL = PageURLs[len(PageURLs) - 1].get('href')
    startIndex = lastPageURL.find('page-')

    # find the page number in url
    totalPageNumber = lastPageURL[startIndex + 5:]
    totalPageNumber = list(totalPageNumber)
    totalPageNumber[len(totalPageNumber) - 1] = ''
    totalPageNumber = ''.join(totalPageNumber)

    return int(totalPageNumber)

def getPostInfo(startURL):
    # download content of cryptocurrencies at tradingview.com
    r = requests.get(startURL)

    # check if the content is valid or not
    if r.status_code != requests.codes.ok:
        return None
    
    # use bs4 to reveal the content with html
    soup = BeautifulSoup(r.text, 'html.parser')

    # find post title by css class
    title = str(soup.select('h1.tv-chart-view__title-name.js-chart-view__name.apply-overflow-tooltip')[0].getText().rstrip())
    # find author name by css class
    author = str(soup.select('span.tv-chart-view__title-user-name')[0].getText().rstrip())
    # find time posted by css class
    seconds = int(float(soup.select('span.tv-chart-view__title-time')[0].get('data-timestamp')))
    timestamp = str(datetime.datetime.fromtimestamp(seconds))
    return {'title':title, 'author':author, 'timestamp':timestamp}


if __name__ == '__main__':
    main()