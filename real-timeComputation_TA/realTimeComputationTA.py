
# coding: utf-8

# In[24]:


import sys
from datetime import datetime
import MySQLdb
import pandas as pd
import numpy as np


# In[54]:


class TooFewArguments(Exception):
    pass
class TooMuchArguments(Exception):
    pass
class indicatorIndexOutOfRange(Exception):
    pass


# In[ ]:


# Functions
def reverseData(data):
    data = data[::-1]
    return data
def getRound(data, decimals = 3):
    return np.around(data, decimals)
def GetDataFromDB(ip, userName, psw, tableName, sql):
    try:
        db = MySQLdb.connect(ip, userName, psw, tableName)
    except:
        print ("Error: unable to connect to DB")
        
    #create a cursor
    cursor = db.cursor()
    results = []
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        print ("Error: unable to fetch data from DB")
    
    return results
    db.close()


# In[ ]:


# Indicators
def SMA(data, period):
    if (type(data) == pd.core.series.Series):
        return data.rolling(window = period).mean()
    else:
        print ("Error: Wrong input, SMA(pandas.core.series.Series, integer)")
        
def EMA(data, period):
    if (type(data) == pd.core.series.Series):
        return data.ewm(span = period, adjust = False).mean()
    else:
        print ("Error: Wrong input, EMA(pandas.core.series.Series, integer)")
        
def RSI(data, period, Uprevious = None, Dprevious = None):
    if (type(data) == pd.core.series.Series):
        delta = data.diff()
        up, down = delta.copy(), delta.copy()
        up[up < 0] = 0
        down[down > 0] = 0
        down = abs(down)
        Up = SMA(up, period)
        Up = Up.fillna(0)
        if(Uprevious == None):
            for i in range(period,len(Up)):
                Up[i] = getRound((Up[i - 1] * (period - 1) + up[i]) / period)
        else:
            Up[period - 1] = getRound((Up[i - 1] * (period - 1) + up[i]) / period)
            
        Down = SMA(down, period)
        Down = Down.fillna(0)
        if(Uprevious == None):
            for i in range(period,len(Down)):
                Down[i] = getRound((Down[i - 1] * (period - 1) + down[i]) / period)
        else:
            Down[period - 1] = getRound((Dprevious * (period - 1) + down[i]) / period)

        rsi = 100 * (Up / (Up + Down))
        rsi = rsi.fillna(0)
        return getRound(rsi, 2), Up, Down
    else:
        print ("Error: Wrong input, RSI(pandas.core.series.Series, integer)")
        
def MACD(data, period = []):
    if (type(data) == pd.core.frame.DataFrame and len(period) == 3):   
        di = (data['High'] + data['Low'] + 2.0 * data['Close']) / 4.0
        ema12 = SMA(di, period[0])
        ema12 = ema12.fillna(0)
        for i in range(period[0] + 1, len(ema12)):
            ema12[i] = (ema12[i - 1] * (period[0] - 1) + di[i] * 2.0) / (period[0] + 1)
    
        ema26 = SMA(di, period[1])
        ema26 = ema26.fillna(0)
        for i in range(period[1] + 1, len(ema26)):
            ema26[i] = (ema26[i - 1] * (period[1] - 1) + di[i] * 2.0) / (period[1] + 1)
    
        dif = ema12 - ema26

        dem = SMA(dif, period[2])
        dem = dem.fillna(0)
        for i in range(period[2] + 1, len(dem)):
            dem[i] = (dem[i - 1] * (period[2] - 1) + dif[i] * 2.0) / (period[2] + 1)
        return {'dif':dif, 'dem':dem}
    else:
        if(type(data) == pd.core.frame.DataFrame):
            print ("Error: Wrong input, MACD(pandas.core.frame.DataFrame, list of integer)")
        elif(len(period) == 3):
            print ("Error: number of content in list do not equal to 3")
            
def KD(data, result = {}):
    close = data['Close'].copy()
    for i in range(0,9):
        close[i] = 0

    data['RSV'] = (( data['Close'] - data['Low'].rolling(window = 9).min()) / (data['High'].rolling(window = 9).max() - data['Low'].rolling(window = 9).min()))
    data['RSV'] = data['RSV'].fillna(0)
    if(not result):
        result = {
            'K9':[0],
            'D9' :[0]
        }
    #calculate everyday's KD
    for i in range(1, len(data.index)):
        K9_value = (1.0/3.0) * data['RSV'][i] + (2.0 / 3.0) * result['K9'][i - 1]
        result['K9'].append(getRound(K9_value, 5))
        D9_value = (2.0/3.0) * result['D9'][i - 1] + (1.0 / 3.0) * result['K9'][i]
        result['D9'].append(getRound(D9_value, 5))
        
    return result


# In[53]:


# real-time computation functions
# RSI
def rsiR(date):
    # pull the last 9 data needed
    period = 9
    sql = "SELECT DATE,LAST FROM btc_usd WHERE DATE <= %s ORDER BY DATE DESC LIMIT %s" % ("'" + str(date) + "'", str(period))
    print(sql)
    result = GetDataFromDB("140.118.126.136", "123", "1234567890", "test", sql)
    sql = "SELECT UP,DOWN FROM indicators WHERE DATE < %s ORDER BY DATE DESC LIMIT 1" % ("'" + str(date) + "'")
    print(sql)
    upResult = GetDataFromDB("140.118.126.136", "123", "1234567890", "test", sql)
    
    u = upResult[0][0]
    d = upResult[0][1]
    
    # organized the data to 2 list
    Dates = []
    ClosePrices = []

    for i in range(len(result)):
        Dates.append(result[i][0])
        ClosePrices.append(result[i][1])
        
    # reorganized the data by reverse order
    Dates = reverseData(Dates)
    ClosePrices = reverseData(ClosePrices)
    
    # reorganized the data to dataframe type
    CoinPriceData = {
        'Date' : Dates,
        'Close' : ClosePrices
    }
    CoinPriceData = pd.DataFrame(CoinPriceData)
    
    # calculate the value of rsi
    period = period - 1
    resultOfrsiR, u, d = RSI(CoinPriceData['Close'], period, u, d);
    targetRSI = getRound(float(resultOfrsiR[len(resultOfrsiR) - 1]))

    # push the result to database
    sql = "UPDATE indicators SET RSI = %s, UP = %s, DOWN = %s WHERE DATE = %s" % (targetRSI, str(u), str(d), date)
    print("rsi = ", targetRSI)
    print(sql)
    result = GetDataFromDB("140.118.126.136", "123", "1234567890", "test", sql)
    
# SMA
def smaR(Date):
    # pull the last 100 data needed
    period = 240
    sql = "SELECT DATE,LAST FROM btc_usd WHERE DATE <= %s ORDER BY DATE DESC LIMIT %s" % ("'" + str(Date) + "'", str(period))
    result240 = GetDataFromDB("140.118.126.136", "123", "1234567890", "test", sql)
    
    # organized the data to 2 list
    Dates = []
    ClosePrices = []
    for i in range(len(result240)):
        Dates.append(result240[i][0])
        ClosePrices.append(result240[i][1])
    
    # reorganized the data by reverse order
    Dates = reverseData(Dates)
    ClosePrices = reverseData(ClosePrices)

    # reorganized the data to dataframe type
    CoinPriceData = {
        'Date' : Dates,
        'Close' : ClosePrices
    }
    CoinPriceData = pd.DataFrame(CoinPriceData)
    
    # calculate the value of rsi
    period = period
    resultOfsma240R = SMA(CoinPriceData['Close'], period);
    targetSMA240 = getRound(float(resultOfsma240R[len(resultOfsma240R) - 1]))
    
    period = 120
    resultOfsma120R = CoinPriceData['Close'].tail(120).reset_index()
    del resultOfsma120R['index']
    resultOfsma120R = SMA(resultOfsma120R['Close'], period);
    targetSMA120 = getRound(float(resultOfsma120R[len(resultOfsma120R) - 1]))
    
    period = 60
    resultOfsma60R = CoinPriceData['Close'].tail(60).reset_index()
    del resultOfsma60R['index']
    resultOfsma60R = SMA(resultOfsma60R['Close'], period);
    targetSMA60 = getRound(float(resultOfsma60R[len(resultOfsma60R) - 1]))
    
    period = 20
    resultOfsma20R = CoinPriceData['Close'].tail(20).reset_index()
    del resultOfsma20R['index']
    resultOfsma20R = SMA(resultOfsma20R['Close'], period);
    targetSMA20 = getRound(float(resultOfsma20R[len(resultOfsma20R) - 1]))
    
    period = 10
    resultOfsma10R = CoinPriceData['Close'].tail(10).reset_index()
    del resultOfsma10R['index']
    resultOfsma10R = SMA(resultOfsma10R['Close'], period);
    targetSMA10 = getRound(float(resultOfsma10R[len(resultOfsma10R) - 1]))
    
    period = 5
    resultOfsma5R = CoinPriceData['Close'].tail(5).reset_index()
    del resultOfsma5R['index']
    resultOfsma5R = SMA(resultOfsma5R['Close'], period);
    targetSMA5 = getRound(float(resultOfsma5R[len(resultOfsma5R) - 1]))
    
    # push the result to database
    sql = "UPDATE indicators SET SMA240 = %s, SMA120 = %s, SMA60 = %s, SMA20 = %s, SMA10 = %s, SMA5 = %s WHERE DATE = %s" % (targetSMA240, targetSMA120, targetSMA60, targetSMA20, targetSMA120, targetSMA5, Date)
    print(sql)
    result = GetDataFromDB("140.118.126.136", "123", "1234567890", "test", sql)
    
# KD
def kdR(date):
    # pull the last 2 data needed
    period = 2
    sql = "SELECT DATE,LAST FROM btc_usd WHERE DATE <= %s ORDER BY DATE DESC LIMIT %s" % ("'" + str(date) + "'", str(period))
    result = GetDataFromDB("140.118.126.136", "123", "1234567890", "test", sql)
    sql = "SELECT K, D FROM indicators WHERE DATE < %s ORDER BY DATE DESC LIMIT %s" % ("'" + str(date) + "'", str(period))
    kdresult = GetDataFromDB("140.118.126.136", "123", "1234567890", "test", sql)
    print(kdresult)
    # organized the data to 2 list
    Dates = []
    ClosePrices = []
    K = []
    D = []
    for i in range(len(result)):
        Dates.append(result[i][0])
        ClosePrices.append(result[i][1])
        K.append(kdresult[i][0])
        D.append(kdresult[i][1])
    
    # reorganized the data by reverse order
    Dates = reverseData(Dates)
    ClosePrices = reverseData(ClosePrices)
    K = reverseData(K)
    D = reverseData(D)
    
    # reorganized the data to dataframe type
    CoinPriceData = {
        'Date' : Dates,
        'Close' : ClosePrices
    }
    kd = {
        'K9' : K,
        'D9' : D
    }
    CoinPriceData = pd.DataFrame(CoinPriceData)
    kd = pd.DataFrame(kd)
    
    # calculate the value of rsi
    resultOfkdR = KD(CoinPriceData, kd);
    targetK = getRound(float(resultOfkdR['K9'][len(resultOfkdR) - 1]))
    targetD = getRound(float(resultOfkdR['D9'][len(resultOfkdR) - 1]))
    
    # push the result to database
    sql = "UPDATE indicators SET K = %s , D = %s WHERE DATE = %s" % (targetK, targetD, date)
    print(sql)
    result = GetDataFromDB("140.118.126.136", "123", "1234567890", "test", sql)
    
# MACD
def macdR(date):
    # pull the last 2 data needed
    # 12 26 9
    period = 26
    sql = "SELECT DATE,LAST, HIGH, LOW FROM btc_usd WHERE DATE <= %s ORDER BY DATE DESC LIMIT %s" % ("'" + str(date) + "'", str(period))
    result = GetDataFromDB("140.118.126.136", "123", "1234567890", "test", sql)
    
    # organized the data to 2 list
    Dates = []
    ClosePrices = []
    HighPrices = []
    LowPrices = []
    for i in range(len(result)):
        Dates.append(result[i][0])
        ClosePrices.append(result[i][1])
        HighPrices.append(result[i][2])
        LowPrices.append(result[i][3])
    
    # reorganized the data to dataframe type
    CoinPriceData = {
        'Date' : Dates,
        'Close' : ClosePrices,
        'High' : HighPrices,
        'Low' : LowPrices
    }
    CoinPriceData = pd.DataFrame(CoinPriceData)
    
    # calculate the value of rsi
    resultOfmacdR = MACD(CoinPriceData, [12, 26, 9]);
    targetDIF = getRound(float(resultOfmacdR['dif'][len(resultOfmacdR) - 1]))
    targetDEM = getRound(float(resultOfmacdR['dem'][len(resultOfmacdR) - 1]))
    
    # push the result to database
    sql = "UPDATE indicators SET DIF = %s , DEM = %s WHERE DATE = %s" % (targetDIF, targetDEM, date)
    print(sql)
    result = GetDataFromDB("140.118.126.136", "123", "1234567890", "test", sql)


# In[50]:


def main():
    try:
        if len(sys.argv) < 4: # too few arguments
            raise TooFewArguments
        elif len(sys.argv) > 4: # too much arguments
            raise TooMuchArguments
        else:
            try:
                indicatorIndex = int(sys.argv[1])
                if indicatorIndex > 3 or indicatorIndex < 0:
                    raise indicatorIndexOutOfRange
                else:
                    try:
                        date = datetime.strptime(sys.argv[2] + " " + sys.argv[3], "%Y-%m-%d %H:%M:%S")
                        # all arguments are defined
                        if indicatorIndex == 0: # update sma
                            print("Updateing sma indicator data at %s..." % date)
                            smaR(date)
                        elif indicatorIndex == 1: # update rsi
                            print("Updateing rsi indicator data at %s..." % date)
                            rsiR(date)
                        elif indicatorIndex == 2: # update kd
                            print("Updateing kd indicator data at %s..." % date)
                            kdR(date)
                        elif indicatorIndex == 3: # update macd
                            print("Updateing macd indicator data at %s..." % date)
                            macdR(date)
                    except ValueError:
                        print("Undefined format of date - [yyyy-mm-dd hh:mm:ss]")
                        sys.exit()
            except ValueError:
                print("Index of indicator is not a number")
                sys.exit()
    except TooFewArguments:
        print("Arguments are too few, realTimeComputationTA [IndicatorsIndex-0 to 3] [yyyy-mm-dd hh:mm:ss]")
        sys.exit()
    except TooMuchArguments:
        print("Arguments are too much, realTimeComputationTA [IndicatorsIndex-0 to 3] [yyyy-mm-dd hh:mm:ss]")
        sys.exit()
    except indicatorIndexOutOfRange:
        print("Index of indicators is out of range - [0,3]")
        sys.exit()


# In[55]:


if __name__ == "__main__":
    main()

