# -*- coding: utf-8 -*-
import scrapy

from tradingviewAnalysis.items import TradingviewanalysisItem

class TradingviewSpider(scrapy.Spider):
    name = 'tradingview'
    allowed_domains = ['tradingview.com']
    #start_urls = ['https://www.tradingview.com/markets/cryptocurrencies/ideas/']
    start_urls = ['https://www.tradingview.com/chart/BTCUSD/EAlOT0eT-What-our-Bitcoin-market-has-become-a-futures-market/']

    def parse(self, response):
        title = response.xpath("//div[@class='tv-chart-view__title selectable--full']//h1/text()").extract_first()
        time = response.xpath("//span[@class='tv-chart-view__title-time']/text()").extract_first()
        author = response.xpath("//span[@class='tv-chart-view__title-user-name']/text()").extract_first()
        author = str(author).rstrip()
        title = str(title)
        yield {'title':title, 'time':time, 'author':author}