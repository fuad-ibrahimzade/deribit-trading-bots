# -*- coding: utf-8 -*-
"""
Created on Mon Aug 20 18:33:28 2018
original qaqasito
@author: Fuad Ibrahimzade
"""

import datetime
import pandas as pd
from urllib2 import Request, urlopen
import json
from pandas.io.json import json_normalize
import numpy as np
from numpy.lib.stride_tricks import as_strided
import time
from deribit_api import RestClient
import base64 , hashlib

import schedule

import threading
import os


def main2():
    data, gotData=get_CryptoCompareData()

    leverage = 1
    
    odir=simpleSTR(data)

    orderDirection=odir
    if orderDirection==-1:
        orderDirection='sell'
    elif orderDirection==1:
        orderDirection='buy'
    else:
        orderDirection='nothing'
        
    
    saveForFlaskNgrok(orderDirection,gotData)
    if orderDirection!='nothing':
        doStrategyAtBroker(orderDirection)
    
def main():
    try:
        main2()
    except Exception:
        a=0
        
def saveForFlaskNgrok(orderDirection,gotData):
    try:     
        infoToSave='strategy signal: '+orderDirection+' and time: '+str(datetime.datetime.now())+' and gotdata: '+str(gotData)
        myDERIBIT.timeLeft-=1
        myDERIBIT.emailSentTime-=1
        infoToSave+=' STOPTIME: '+str(myDERIBIT.timeLeft)
        myDERIBIT.lastInfo=infoToSave
        if myDERIBIT.emailSentTime==0:
            sendMail("qaqulyadata",infoToSave)
            myDERIBIT.emailSentTime=5
        if myDERIBIT.timeLeft==0:
            myDERIBIT.timeLeft=500*60
            schedule.cancel_job(main)
            schedule.clear('tagim-1')
            closeDERIBIT()
    except Exception:
        a=0
        
def closeDERIBIT():
    brokerim=myDERIBIT()
    openPosParams, openPosDirection = brokerim.getPositionsWithSlippage()
    brokerim.ClosePosition(openPosParams,openPosDirection)
    brokerim=0 
    

def doStrategyAtBroker(strategySignal):
    brokerim=myDERIBIT()
    ask, bid = brokerim.getBestBidAsk()
    ask=str(ask)
    bid=str(bid)
    posParams={
            'instrument':brokerim.frontMfuture,
            'quantity':brokerim.contractCount,
            'price':str(0)
            }
    posParams['price']=str(ask) if strategySignal=='buy' else str(bid)
    direction=strategySignal
    
    openPosParams, openPosDirection = brokerim.getPositionsWithSlippage()
    if (strategySignal=='buy' or strategySignal=='sell') and openPosDirection==0:
        brokerim.BuyOrSellMarket(posParams,direction)
    if (strategySignal=='buy' and openPosDirection=='sell') or (strategySignal=='sell' and openPosDirection=='buy') or (openPosDirection!=0 and posParams['quantity']!=openPosParams['quantity']):
        brokerim.ClosePosition(openPosParams,openPosDirection)
        brokerim.BuyOrSellMarket(posParams,direction)
        
    
    brokerim=0

       
def simpleSTR(df):
    per=40
    donchH= df['high'].shift(1).rolling(window=per).max()
    donchL= df['low'].shift(1).rolling(window=per).min()

    basePrice=df.at[1,'open']
    condition=np.where(df['close']>0,0,0)
    newday=False
    found=False
    bas=donchH.at[1]
    dib=donchL.at[1]
    bas2=donchH.at[1]
    dib2=donchL.at[1]
    num=0
    buy=False
    sell=False
    orderDirection=0
    average_true_range(df,14)
    for i in xrange(len(df.index)):
        newDay=("2018 22:00" in getCurrentForTime(df.at[i,'time'])) or ("2018 22:01" in getCurrentForTime(df.at[i,'time']))
        if newDay:
            basePrice=df.at[i,'open']
            found=True
            num=-10000
            bas=donchH.at[i]
            dib=donchL.at[i]
            bas2=donchH.at[i]
            dib2=donchL.at[i]
            buy=False
            sell=False
            
#        if i>560 and found==False:
#            found=True
#            bas=donchH.at[i]
#            dib=donchL.at[i]
#            bas2=donchH.at[i]
#            dib2=donchL.at[i]
#            buy=False
#            sell=False
            
            
        if  found==False:
            continue
        if df.at[i,'high']>bas:
            condition[i]=1
            orderDirection=condition[i]
#            print('signal: buy and date: '+getCurrentForTime(df.at[i,'time'])+' and i '+str(i))
            bas=df.at[i,'high']
#            dib2=df.at[i,'low']
            dib2=donchL.at[i]
            buy=True
            sell=False
        if dib2<donchL.at[i] and buy:
            dib2=donchL.at[i]
        if df.at[i,'low']<dib:
            condition[i]=-1
            orderDirection=condition[i]
#            print('signal: sell and date: '+getCurrentForTime(df.at[i,'time'])+' and i '+str(i))
            dib=df.at[i,'low']
#            bas2=df.at[i,'high']
            bas2=donchH.at[i]
            buy=False
            sell=True
        if bas2>donchH.at[i] and sell:
            bas2=donchH.at[i]
            
            
        if buy and df.at[i,'low']<dib2 and (basePrice-df.at[i,'close'])<df.at[i,'atr']:
            condition[i]=-1
            orderDirection=condition[i]
#            print('signal: sell and date: '+getCurrentForTime(df.at[i,'time'])+' and i '+str(i))
            buy=False
            sell=False
        if sell and df.at[i,'high']>bas2 and (basePrice-df.at[i,'close'])>-df.at[i,'atr']:
            condition[i]=1
            orderDirection=condition[i]
#            print('signal: buy and date: '+getCurrentForTime(df.at[i,'time'])+' and i '+str(i))
            buy=False
            sell=False            
#        if buy:
#            condition[i]=1
#        if sell:
#            condition[i]=-1
            
        
            
#    print("latest time: "+getCurrentForTime(df.at[len(df.index)-1,'time']))
    df['signalUpOrDown']=condition
    df['signalUpOrDown']=df['signalUpOrDown'].replace(to_replace=0, method='ffill')
#    df['signalUpOrDown']=df['signalUpOrDown'].replace(to_replace=9, value=0)
#    df['signalUpOrDown']=df['signalUpOrDown'].replace(to_replace=-9, value=0)
#    df['signalUpOrDown']=df['signalUpOrDown'].replace(to_replace=0, method='ffill')
#    df['signalUpOrDown']=df['signalUpOrDown'].replace(to_replace=-9, method='ffill')
#    df['signalUpOrDown']=df['signalUpOrDown'].replace(to_replace=9, method='ffill')
#    df['signalUpOrDown']=df['signalUpOrDown'].shift(-1)
    return orderDirection
 
    
def average_true_range(data, trend_periods=14, open_col='open', high_col='high', low_col='low', close_col='close', drop_tr = True):
    for i in data.index:
        prices = [data.at[i,high_col], data.at[i,low_col], data.at[i,close_col], data.at[i,open_col]]
        if i > 0:
            val1 = np.amax(prices) - np.amin(prices)
            val2 = abs(np.amax(prices) - data.at[i - 1, close_col])
            val3 = abs(np.amin(prices) - data.at[i - 1, close_col])
            true_range = np.amax([val1, val2, val3])

        else:
            true_range = np.amax(prices) - np.amin(prices)

        data.at[i, 'true_range']=true_range
    data['atr'] = data['true_range'].ewm(ignore_na=False, min_periods=0, com=trend_periods, adjust=True).mean()
    if drop_tr:
        data = data.drop(['true_range'], axis=1)
        
    return data


    
def get_CryptoCompareData():
    gotData=False
    btcLink='https://min-api.cryptocompare.com/data/histominute?fsym=BTC&tsym=USD&limit=2000&aggregate=5'
    try:
        request=Request(btcLink)
        response = urlopen(request)
        elevations = response.read()
        data2 = json.loads(elevations)
        data2=json_normalize(data2['Data'])
        data2=pd.DataFrame.from_dict(data2)
        gotData=True
        return data2, gotData
    except Exception:
        gotData=False
    
def maxDrawDown(ser):
    cum = ser.cumsum()
    highV = cum.cummax()

    drowdawn = cum - highV
    return drowdawn

def timeAndCloseToFloat(df):
    df["time"]=df["time"].apply(parse_float)
    df["close"]=df["close"].apply(parse_float)
    return df

def parse_float(x):
    try:
        x = float(x)
        if x>10000:
            x=datetime.datetime.fromtimestamp(x)
    except Exception:
        x = 0
    return x

def getCurrentForTime(timeSt):
    timestampim=timeSt
    timestampim=time.strftime("%a %d %b %Y %H:%M:%S GMT", time.gmtime(timestampim / 1.0)) 
    return timestampim

def checkLastTimeOfDF(data):
    timestampim=data['time'].iloc[-1]
    timestampim=time.strftime("%a %d %b %Y %H:%M:%S GMT", time.gmtime(timestampim / 1))
    return timestampim

class myHeroku:
    myLink='no'
    otherLink='no'
    minutesleft=25
    dynoMinutesleft=450*60
#    def __init__(self):
#        self.myLink= os.environ.get('myLink', 'no')
#        self.otherLink= os.environ.get('otherLink', 'no')
        
    def goToMyLink():
        try:
            request=Request(myHeroku.myLink)
            urlopen(request)
        except Exception:
            a=0
            
    def goToOtherLink():
        try:
            request=Request(myHeroku.otherLink)
            urlopen(request)
        except Exception:
            a=0
           
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
def sendMail(subject, text):
    to=os.environ.get('toEmail', 'no')
    gmail_user=os.environ.get('gmail_user', 'no')
    gmail_pwd=os.environ.get('gmail_pwd', 'no')
    msg = MIMEMultipart()
    msg['From'] = gmail_user
    msg['To'] = to
    msg['Subject'] = subject
    msg.attach(MIMEText(text))
    mailServer = smtplib.SMTP("smtp.gmail.com", 587)
    mailServer.ehlo()
    mailServer.starttls()
    mailServer.ehlo()
    mailServer.login(gmail_user, gmail_pwd)
    mailServer.sendmail(gmail_user, to, msg.as_string())
    mailServer.close()
        
    
class myDERIBIT:
    Access_key="no"
    Access_secret="no"
    client=0
    frontMfuture=0
    lastInfo='nothing yet'
    killDeribitThread=False
    contractCount=str(1)
    timeLeft=500*60
    emailSentTime=5
    
    def __init__(self):
        self.Access_key=os.environ.get('Access_key', 'no')
        self.Access_secret=os.environ.get('Access_secret', 'no')
        self.contractCount=os.environ.get('count', 'no')
        self.client = RestClient(self.Access_key, self.Access_secret)
        self.frontMfuture=self.getFronFutureName()
        timestampim=self.getLastTradetimeToDate()
        
    def ClosePosition(self,posParams,direction):
        closeDirection=0
        if direction=='buy':
            closeDirection='sell'
        elif direction=='sell':
            closeDirection='buy'
        self.BuyOrSellMarket(posParams,closeDirection)
        
    def BuyOrSellMarket(self,posParams,direction):
        response=0
        posParams['type']='market'
        posParams['price']=''
        
        
        if direction=='buy':
            try:
                response=self.client.buy(posParams['instrument'],posParams['quantity'],posParams['price'])
            except Exception:
                nonce= int(time.time()* 1000)
                signature=self.deribit_signature(nonce,'/api/v1/private/buy',posParams,self.Access_key,self.Access_secret)
                response = self.client.session.post('https://www.deribit.com' + '/api/v1/private/buy', data=posParams, headers={'x-deribit-sig': signature}, verify=True)
        elif direction=='sell':
            try:
                self.client.sell(posParams['instrument'],posParams['quantity'],posParams['price'])
            except Exception:
                nonce= int(time.time()* 1000)
                signature=self.deribit_signature(nonce,'/api/v1/private/sell',posParams,self.Access_key,self.Access_secret)
                response = self.client.session.post('https://www.deribit.com' + '/api/v1/private/sell', data=posParams, headers={'x-deribit-sig': signature}, verify=True)

    def getPositionsWithSlippage(self):
        posParams={}
        posJson=0
        direction=0
        try:
            posJson=self.value(self.client.positions())
        except Exception:
            nonce= int(time.time()* 1000)
            signature=self.deribit_signature(nonce,'/api/v1/private/positions',posParams,self.Access_key,self.Access_secret)
            response = self.client.session.post('https://www.deribit.com' + '/api/v1/private/positions', data=posParams, headers={'x-deribit-sig': signature}, verify=True)
            posJson=self.value(response.json())['result']
        
        if posJson.empty:
            direction=0
        else:
            posParams['instrument']=str(posJson['instrument'][0])
            direction=str(posJson['direction'][0])
            posParams['quantity']=str(abs(posJson['size'][0]))
            if direction=='buy':
                posParams['price']=str(posJson['markPrice'][0]-5)
            elif direction=='sell':
                posParams['price']=str(posJson['markPrice'][0]+5)
        return posParams,direction
        
    def deribit_signature(self,nonce, uri, params, access_key, access_secret):
        sign = '_=%s&_ackey=%s&_acsec=%s&_action=%s' % (nonce, access_key, access_secret, uri)
        for key in sorted(params.keys()):
            sign += '&' + key + '=' + "".join(params[key])
        return '%s.%s.%s' % (access_key, nonce, base64.b64encode(hashlib.sha256(sign).digest()))
    
    def value(self,df):
        return json_normalize(df)
    
    def getFronFutureName(self):
        instruments=self.client.getinstruments()
        instruments0=json_normalize(instruments)
        instruments0=pd.DataFrame.from_dict(instruments)
        futures=instruments0[instruments0['kind'].str.contains("uture")]
        frontMfuture=futures.iloc[0]['instrumentName']
        now=datetime.datetime.now()
        for i in xrange(len(futures)):
            notPERPETUAL='PERPETUAL' not in futures.iloc[i]['instrumentName']
            yearDif=int(futures.iloc[i]['expiration'][0:4])-int(now.year)
            monthDif=int(futures.iloc[i]['expiration'][5:7])-int(now.month)
            dayDif=int(futures.iloc[i]['expiration'][8:10])-int(now.day)
            if notPERPETUAL and yearDif==1:
                frontMfuture=futures.iloc[i]['instrumentName']
                break
            if notPERPETUAL and yearDif==0 and monthDif>0:
                frontMfuture=futures.iloc[i]['instrumentName']
                break
            if notPERPETUAL and monthDif==0 and dayDif>4:
                frontMfuture=futures.iloc[i]['instrumentName']
                break
        return frontMfuture
    
    def getLastTradetimeToDate(self,instrument=None):
        if instrument==None:
            instrument=self.frontMfuture
        timestampim=(self.value(self.client.getlasttrades(instrument))['timeStamp'][0])
        timestampim=time.strftime("%a %d %b %Y %H:%M:%S GMT", time.gmtime(timestampim / 1000.0))    
        return timestampim
    
    def getBestBidAsk(self,instrument=None):
        if instrument==None:
            instrument=self.frontMfuture
        ask=self.value(self.client.getorderbook(instrument)['asks'][0])['price']# satmaq isdiyenner
        bid=self.value(self.client.getorderbook(instrument)['bids'][0])['price']#almaq istiyenner
        return ask,bid


#@with_goto
def goto():
#    label .begin
    seconds=60

    try:
        main()
        schedule.every(seconds).seconds.do(main).tag('tagim-1')
        while 1:                           
            schedule.run_pending()
            time.sleep(1)
#        while 1:
#            if myDERIBIT.killDeribitThread:
#                schedule.cancel_job(main)
#                schedule.clear('tagim-1')
##                print('cixdix')
#                raise SystemExit
##                os._exit()
#                break
#            myHeroku.minutesleft-=1
#            myHeroku.dynoMinutesleft-=1
#            if myHeroku.minutesleft==0:
#                myHeroku.minutesleft=25
#                menimHerokum=myHeroku()
#                menimHerokum.goToMyLink()
#            if myHeroku.dynoMinutesleft==0:
#                myHeroku.dynoMinutesleft=450*60
#                myHeroku.minutesleft=25
#                menimHerokum=myHeroku()
#                menimHerokum.goToOtherLink()
#                myDERIBIT.killDeribitThread=True
#                schedule.cancel_job(main)
#                schedule.clear('tagim-1')
#                raise SystemExit
#                break                            
#            schedule.run_pending()
#            time.sleep(1)
    except Exception:
        schedule.cancel_job(main)
        schedule.clear('tagim-1')
        if myDERIBIT.killDeribitThread:
            pass
#        goto .begin
        
        
#from flask import Flask
#app = Flask(__name__)
#@app.route('/')
def hello_world():
#    now=datetime.datetime.now()
#    return 'Hello, World! '+str(now)
    stringtoPrint=''
    try:
        stringtoPrint=readForFlaskNgrok()
    except Exception:
        stringtoPrint=''

    return stringtoPrint



#@app.before_first_request
#def activate_job():
#    thread = threading.Thread(target=goto)
#    thread.start()
        
#@app.route('/kill')
def kill():
    myDERIBIT.killDeribitThread=True
    return 'killed'
  
#@app.route('/start')    
def start():
    if myDERIBIT.killDeribitThread:
        myDERIBIT.killDeribitThread=False
        thread = threading.Thread(target=goto)
        thread.start()
    return 'started'

def readForFlaskNgrok():      
    infoRead=myDERIBIT.lastInfo
    return infoRead

#def flaskRunner():
#    app.run()
        
if __name__ == '__main__':
#    atexit.register(lambda: schedule.clear('tagim-1'))
    goto()
    
#    thread = threading.Thread(target=goto)
#    thread.start()
#    try:
#        app.run(threaded=True)
#    except Exception:
##        print('bbbbbb')
#        myDERIBIT.killDeribitThread=True
#        os._exit(1)
    
    