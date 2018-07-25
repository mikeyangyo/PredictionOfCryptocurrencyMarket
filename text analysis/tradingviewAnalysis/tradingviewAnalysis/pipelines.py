# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from tradingviewAnalysis.models import db_connect

class TradingviewanalysisPipeline(object):
    def __init__(self):
        self.connection = db_connect()
    
    def process_item(self, item, spider):
        '''
        quote = {
            "title": item["title"],
            "author": item["author"],
            #"content": item["content"],
            "time" : item["timestamp"]
        }

        self.connection.insert(quote)
        return item
        '''
        pass