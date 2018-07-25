# -*- coding: utf-8 -*-
import scrapy


class TradingviewSpider(scrapy.Spider):
    name = 'tradingview'
    allowed_domains = ['tradingview.com']
    start_urls = ['http://tradingview.com/']

    def parse(self, response):
        pass
