from pymongo import MongoClient
from scrapy.utils.project import get_project_settings

def db_connect():
    settings = get_project_settings()
    connection = MongoClient(settings['MONGODB_SERVER'],settings['MONGODB_PORT'])
    collection = connection[settings['MONGODB_DB']][settings['MONGODB_COLLECTION']]
    return collection