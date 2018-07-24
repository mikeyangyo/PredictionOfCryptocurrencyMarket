# -*- coding: utf-8 -*-
from __future__ import print_function
import json
import re
import logging

import scrapy
from scrapy.http.request import Request
from reddit.items import RedditItem
from six.moves.urllib import parse

class RedditspiderSpider(scrapy.Spider):
    name = 'redditspider'
    #allowed_domains = ['reddit.com']
    #start_urls = ['http://reddit.com/']
    #allowed_domains = ['quotes.toscrape.com']
    #start_urls = ['http://quotes.toscrape.com/']

    def start_requests(self):
        yield Request(
            url = "https://scrapingclub.com/exercise/list_infinite_scroll/",
            callback = self.parse_list_page
        )

    '''
    def parse(self, response):
        # parse each quote in this page
        for quote in response.xpath("//div[@class='quote']"):
            text = quote.xpath(".//span[@class='text']/text()").extract()
            author = quote.xpath(".//small[@class='author']/text()").extract()

            item = RedditItem()
            item['author'] = author
            item['text'] = text

            yield item
        # go to next page if we parsed all quotes in this page
        #get url of next page
        nextPageUrl = response.xpath("//li[@class='next']//a/@href").extract_first()
        if nextPageUrl:
            absoluteNextPageUrl = response.urljoin(nextPageUrl)
            yield scrapy.Request(absoluteNextPageUrl)
    '''

    def parse_list_page(self, response):
        """
        The url of next page is like
        https://scrapingclub.com/exercise/list_infinite_scroll/?page=2

        It can be found in a.next-page
        """
        #First, check if next page available, if found, yield request
        nextLink = response.xpath("//a[@class='page-link next-page']/@href").extract_first()
        if nextLink:
            # If the website has strict policy, you should do more work here
            # Such as modifying HTTP headers

            # concatenate url
            url = response.url
            nextLink = url[:url.find('?')] + nextLink
            yield Request(
                url = nextLink,
                callback = self.parse_list_page
            )
        
        # find product link and yield request back
        for req in self.extract_product(response):
            yield req
    
    def extract_product(self, response):
        links = response.xpath("//div[@class='col-lg-8']//div[@class='card']/a/@href").extract()
        for url in links:
            result = parse.urlparse(response.url)
            baseUrl = parse.urlunparse((result.scheme, result.netloc, "", "", "", ""))
            url = parse.urljoin(baseUrl, url)
            yield Request(
                url = url,
                callback = self.parse_product_page
            )

    def parse_product_page(self, response):
        """
        The product page use ajax to get the data, try to analyze it and finish it
        by yourself.
        """
        logging.info("processing " + response.url)
        #infos = response.xpath("//div[@class='col-lg-8']//div[@class='card-body']")
        titles = response.xpath("//div[@class='col-lg-8']//div[@class='card-body']//h3[@class='card-title']/text()").extract()
        '''
        for info in infos:
            title = info.xpath(".//h3[@class='card-title']/text()").extract()
            price = info.xpath(".//h4[@class='card-price']/text()").extract()
            description = info.xpath(".//p[@class='card-description']/text()").extract()

            productInfo = RedditItem()
            productInfo["title"] = title
            productInfo["price"] = price
            productInfo["description"] = description
            yield productInfo
            #yield {'title':title, 'price':price, 'description':description}
        '''
        yield {"title":titles}