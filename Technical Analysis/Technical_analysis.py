
# coding: utf-8

# In[14]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pymysql


# In[15]:


# Functions

## given two curve a, b, then return the inversection point of two curves
def FindTheBuyPoint(a, b):
    buyPoint = np.argwhere(np.diff(np.sign(a - b)) > 0).reshape(-1) + 0
    return buyPoint

def FindTheSellPoint(a, b):
    sellPoint = np.argwhere(np.diff(np.sign(a - b)) < 0).reshape(-1) + 0
    return sellPoint


# In[16]:


#get the close price from DB

#connect to database
try:
    db = pymysql.connect("140.118.126.136", "123", "1234567890", "test")
except:
    print("Error: unable to connect to DB")
    
#create a cursor
cursor = db.cursor()

#try to search all close prices in DB, and change the type to list
sql = "SELECT LAST, DATE FROM btc_usd ORDER BY DATE"

try:
    cursor.execute(sql)
    results = cursor.fetchall()
    
    Dates = []
    ClosePrices = []
    for i in range(len(results)):
        Dates.append(results[i][1])
        ClosePrices.append(results[i][0])
    
    closePriceData = {
        'Date' : Dates,
        'Close' : ClosePrices
    }
except:
    print ("Error: unable to fetch data from DB")
    
db.close()
dfs_close = pd.DataFrame(closePriceData)


# In[17]:


# sma
sma5_close = dfs_close['Close'].rolling(window = 5).mean()
sma20_close = dfs_close['Close'].rolling(window = 20).mean()
sma100_close = dfs_close['Close'].rolling(window = 100).mean()


# In[18]:


# ema
ema12_close = dfs_close['Close'].ewm(span = 12).mean()
ema26_close = dfs_close['Close'].ewm(span = 26).mean()


# In[19]:


# Draw SMA curve
totalDate = 365
plt.figure(figsize = (16, 8))

x = [i for i in range(len(dfs_close.head(totalDate)))]
#x = list(dfs['Date'].head(totalDate))

## draw sma5
sma5c = [sma5_close[i] for i in range(len(sma5_close.head(totalDate)))]
plt.plot(x, sma5c, 'r', label = 'sma_5 days')

## draw sma20
sma20c = [sma20_close[i] for i in range(len(sma20_close.head(totalDate)))]
plt.plot(x, sma20c, 'g', label = 'sma_20 days')

## draw sma100
sma100c = [sma100_close[i] for i in range(len(sma100_close.head(totalDate)))]
plt.plot(x, sma100c, 'b', label = 'sma_100 days')

## draw all close prices
plt.plot(x, dfs_close['Close'].head(totalDate), label = 'Close price', color = 'k')

## draw the intersection points of three curves
# FindTheIntersection(sma5c, sma20c)

#p#icture setting
plt.xlabel("Index")
plt.ylabel("Close Prices")
plt.title("Technical Analysis - Simple Moving Average")
plt.legend(loc = 'upper right')

##show picture
plt.show()


# In[20]:


# Draw EMA curve
totalDate = 365
plt.figure(figsize = (16, 8))

x = [i for i in range(len(dfs_close.head(totalDate)))]
#x = list(dfs['Date'].head(totalDate))

##draw ema12
ema12c = [ema12_close[i] for i in range(len(ema12_close.head(totalDate)))]
plt.plot(x, ema12c, 'r', label = 'ema_12 days')

##draw ema26
ema26c = [ema26_close[i] for i in range(len(ema26_close.head(totalDate)))]
plt.plot(x, ema26c, 'g', label = 'ema_26 days')

##draw all close prices
plt.plot(x, dfs_close['Close'].head(totalDate), label = 'Close price', color = 'k')

#p#icture setting
plt.xlabel("Index")
plt.ylabel("Close Prices")
plt.title("Technical Analysis - Exponential Moving Average")
plt.legend(loc = 'upper right')

##show picture
plt.show()


# In[21]:


##compare EMA & SMA

# Draw EMA curve
totalDate = 365
plt.figure(figsize = (16, 8))
plt.subplot(3,1,1)
x = [i for i in range(len(dfs_close.head(totalDate)))]
#x = list(dfs['Date'].head(totalDate))

##draw ema5
ema5_close = dfs_close['Close'].ewm(span = 5).mean()
ema5c = [ema5_close[i] for i in range(len(ema5_close.head(totalDate)))]
plt.plot(x, ema5c, 'r', label = 'ema_5 days')

##draw sma5
plt.plot(x, sma5c, 'g', label = 'sma_5 days')

plt.subplot(3,1,2)

##draw ema20
ema20_close = dfs_close['Close'].ewm(span = 20).mean()
ema20c = [ema20_close[i] for i in range(len(ema20_close.head(totalDate)))]
plt.plot(x, ema20c, 'r', label = 'ema_20 days')

##draw sma20
plt.plot(x, sma20c, 'g', label = 'sma_20 days')

plt.subplot(3,1,3)

##draw ema100
ema100_close = dfs_close['Close'].ewm(span = 100).mean()
ema100c = [ema100_close[i] for i in range(len(ema100_close.head(totalDate)))]
plt.plot(x, ema100c, 'r', label = 'ema_100 days')

##draw sma100
plt.plot(x, sma100c, 'g', label = 'sma_100 days')

#picture setting
plt.xlabel("Index")
plt.ylabel("Close Prices")
plt.legend(loc = 'upper right')

##show picture
plt.show()


# In[22]:


# get the High & Low Price from DB
#connect to database
try:
    db = pymysql.connect("140.118.126.136", "123", "1234567890", "test")
except:
    print("Error: unable to connect to DB")
    
#create a cursor
cursor = db.cursor()

#try to search all close prices in DB, and change the type to list
sql = "SELECT HIGH, LOW, DATE FROM btc_usd ORDER BY DATE"

try:
    cursor.execute(sql)
    results = cursor.fetchall()
    
    Dates = []
    HighPrices = []
    LowPrices = []
    for i in range(len(results)):
        Dates.append(results[i][2])
        HighPrices.append(results[i][0])
        LowPrices.append(results[i][1])
    
    PriceData = {
        'Date' : Dates,
        'High' : HighPrices,
        'Low' : LowPrices
    }
except:
    print ("Error: unable to fetch data from DB")
    
db.close()
dfs_HL = pd.DataFrame(PriceData)


# In[23]:


# KD
dfs_HL['RSV'] = 100 * (( dfs_close['Close'] - dfs_HL['Low'].rolling(window = 9).min()) / (dfs_HL['High'].rolling(window = 9).max() - dfs_HL['Low'].rolling(window = 9).min()))

dfs_HL['RSV'].fillna(method = 'bfill', inplace = True)

data = {
    'K9':[17],
    'D9':[39]
}

#calculate everyday's KD
for i in range(1, len(dfs_HL.index)):
    K9_value = (1.0/3.0) * dfs_HL['RSV'][i] + (2.0 / 3.0) * data['K9'][i - 1]
    data['K9'].append(K9_value)
    D9_value = (2.0/3.0) * data['D9'][i - 1] + (1.0 / 3.0) * data['K9'][i]
    data['D9'].append(D9_value)
    
dfs_KD = pd.DataFrame(data)


# In[24]:


# Draw the KD Curve
totalDate = 365
plt.figure(figsize = (16, 8))

x = [i for i in range(len(dfs_close.head(totalDate)))]
#x = list(dfs['Date'].head(totalDate))

## Draw K curve
Kc = [dfs_KD['K9'][i] for i in range(len(dfs_KD['K9'].head(totalDate)))]
plt.plot(x, Kc, 'b', label = 'K curve')

## Draw D curve
Dc = [dfs_KD['D9'][i] for i in range(len(dfs_KD['D9'].head(totalDate)))]
plt.plot(x, Dc, 'G', label = 'D curve')

##draw all intersection point of two curves
#buyPoint = FindTheBuyPoint(Dc, Kc)
#sellPoint = FindTheSellPoint(Dc, Kc)

#p#icture setting
plt.xlabel("Index")
plt.ylabel("Percentages")
plt.title("Technical Analysis - Stochastic Oscillator, KD")
plt.legend(loc = 'upper right')

##show picture
plt.show()


# In[25]:


# RSI
period = 14
delta = dfs_close['Close'].diff()

up, down = delta.copy(), delta.copy()

up[up < 0] = 0
down[down > 0] = 0

rUp = up.rolling(window = period).sum() / period
rDown = down.rolling(window = period).sum() / period * -1

rsi2 = 100 * (rUp / (rUp + rDown))
rsi1 = rsi2.fillna(0)
#rsi2


# In[26]:


# Draw RSI curve
totalDate = 365
plt.figure(figsize = (16, 8))

x = [i for i in range(len(dfs_close.head(totalDate)))]
#x = list(dfs['Date'].head(totalDate))

## Draw rsi curve
rsic = plt.plot(x, rsi2.head(totalDate), 'k', label = 'RSI_curve')

#p#icture setting
plt.xlabel("Index")
plt.ylabel("Percentages")
plt.title("Technical Analysis - Relative Strength Index, RSI")
plt.legend(loc = 'upper right')

##show picture
plt.show()


# In[27]:


# MACD
dif = ema12_close - ema26_close
dem = dif.ewm(span = 9).mean()


# In[28]:


# draw MACD Curve
totalDate = 365
plt.figure(figsize = (16, 8))

x = [i for i in range(len(dfs_close.head(totalDate)))]
#x = list(dfs['Date'].head(totalDate))

## Draw DIF curve
difc = plt.plot(x, dif.head(totalDate), 'k', label = 'DIF_curve')

## Draw DEM curve
demc = plt.plot(x, dem.head(totalDate), 'b', label = 'DEM_curve')

## Draw the intersection points of two curves
buyPoint = FindTheBuyPoint(dif.head(totalDate), dem.head(totalDate))
sellPoint = FindTheSellPoint(dif.head(totalDate), dem.head(totalDate))
buyPoints = [buyPoint[i] for i in range(len(buyPoint))]
sellPoints = [sellPoint[i] for i in range(len(sellPoint))]

plt.plot(buyPoints, dem[buyPoints], "ro", label = "Buy Points")
plt.plot(sellPoints, dem[sellPoints], "go", label = "Sell Points")

#picture setting
plt.xlabel("Index")
plt.ylabel("Close Price")
plt.title("Technical Analysis - Moving Average Convergence / Divergence, MACD")
plt.legend(loc = 'upper right')

##show picture
plt.show()

