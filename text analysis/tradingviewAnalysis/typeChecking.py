import argparse

from crawlerFunctions import getMarkets
from datetime import datetime

def dateChecking(requiredDate):
    try:
        return datetime.strptime(requiredDate, '%Y-%m-%d')
    except ValueError:
        msg = 'Not a valid date: {0}'.format(requiredDate)
        raise argparse.ArgumentTypeError(msg)

def marketChecking(requiredMarket):
    markets = getMarkets()
    try:
        if requiredMarket in markets:
            return requiredMarket
        else:
            raise ValueError

    except ValueError:
        msg = 'Not a valid market: {0}'.format(requiredMarket)
        raise argparse.ArgumentTypeError(msg)