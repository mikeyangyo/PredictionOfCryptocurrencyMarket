#!/usr/bin/python
# coding:utf-8

import requests
import datetime
import string
import lxml.html
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
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
        URLs.append(absoluteURL)
        print absoluteURL
        print getPostInfo(absoluteURL)
        print '=================================================================='

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

    # scroll the page to buttom
    browser = webdriver.Chrome()
    browser.get(startURL)
    time.sleep(1)
    elem = browser.find_element_by_tag_name('body')

    lastHeight = browser.execute_script("return document.body.scrollHeight")
    while True:
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.2)
        newHeight = browser.execute_script("return document.body.scrollHeight")
        if newHeight == lastHeight:
            break
        lastHeight = newHeight

    soup = BeautifulSoup(browser.page_source, 'html.parser')

    #find comment info
    allComments = soup.select('div.tv-chart-comment__wrap')
    for comment in allComments:
        print getCommentInfo(comment)
    
    browser.close()
    return {'title':title, 'author':author, 'timestamp':timestamp}

def getCommentInfo(tagString):
    # get author name of comment
    author = str(tagString.select('span.tv-chart-comment__user-name')[0].getText().strip())
    
    # get comment time posted
    seconds = int(float(tagString.select('span.tv-chart-comment__time')[0].get('data-timestamp')))
    timestamp = str(datetime.datetime.fromtimestamp(seconds))
    
    # get comment content
    content = ''
    toWhom = None
    commentText = tagString.select('div.tv-chart-comment__text')[0].getText().strip()
    
    if commentText != '':
        html = lxml.html.fromstring(commentText)
        content = lxml.html.tostring(html)
    
        # remove the <p> tag
        content = content[3:len(content)-4].strip()
        
        # check if this comment is a reply or not
        if content[0] == '@':
            toWhomEndIndex = content.find(',')
            toWhom = content[1:toWhomEndIndex]
            content = content[toWhomEndIndex + 1:]
    
    # get agree number
    agreeNumTag = tagString.select('span.tv-chart-comment__rating.js-chart-comment__agree.apply-common-tooltip.tv-chart-comment__rating--positive.tv-chart-comment__rating--button')
    agreeNum = 0
    if len(agreeNumTag) != 0:
        agreeNum = int(agreeNumTag[0].getText().rstrip())

    return {'author': author, 'timestamp': timestamp, 'content': content, '# of agree': agreeNum, 'toWhom': toWhom}

if __name__ == '__main__':
    main()