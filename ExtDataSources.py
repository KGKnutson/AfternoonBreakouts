import os
import sys
import ntpath
import six
import traceback
import socket
#import urllib.request
import urllib
from urllib.request import urlopen
import json
from alpha_vantage.timeseries import TimeSeries
import yfinance as yf
import numpy as np
from datetime import datetime, timedelta
import time
import requests
import pandas as pd

DEFAULT_THOD = "9:00:00"
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

class ExtDataSources(object):
    dContacts = {}

    def __init__(self):
        self.dContacts["LEETRADES"] = {
                                   "Bryce":"4257375119@tmomail.net",
                                   "Kevin":"5096305967@vzwpix.com",
                                   }
        self.dContacts["LENOVOTHINKPAD"] = {
                                   "Kenny":"2532088796@vzwpix.com",
                                   "Kevin":"5096305967@vzwpix.com",
                                   }
                                   
        self.dContacts["ALERT"] = {
                                  #"Kevin2":"5096305967@vtext.com",
                                  "Kevin":"5096305967@vzwpix.com",
                                  "Kenny":"2532088796@vzwpix.com",
                                  #"Eric":"2084779481@vtext.com",
                                  #"Kara":"5096991480@vtext.com",
                                  #"Thomas": "2089948383@text.republicwireless.com"
                                  #"Bryce":"4257375119@tmomail.net"
                                  #"Jenna":"5099542644@vtext.com"
                                  }
        self.dContacts["SUPPORT"] = {
                                     "Kevin":"5096305967@vzwpix.com",
                                    }
        self.DT = str(datetime.now()).split(' ')[0]
        #self.DT = str(datetime.today() - timedelta(days=1)).split(' ')[0] #look for previous day's results option#
        #self.DT = "2020-05-29"   #USE THIS OVERRIDE WHEN RUNNING ON THE WEEKEND#
        self.triggerTime = self.DT + " " + "09:31:00"  #alpha_vantage uses 9:31
        #print(self.triggerTime)

    '''
    TickerList = List of publically traded symbols to monitor
    interval = Obsolete #Hard-coded to 60 second interval#
    Saves minute snapshots of volume and price data to a file called "<symbol>_minute_Data"
    Meant to be run during market hours of 7:30 mt to 2:00 mt
    '''
    def saveMinuteStockData(self, tickerList=["UDOW","SDOW"], offset="0"):
        db_file = os.environ.get("StockPriceDB")
        head, tail = ntpath.split(db_file)
        yFinanceList = []
        lastVolume = {}
        underlying = None
        RUNPERIOD = "REGULAR"   ##"REGULAR or POST or PREPRE or POSTPOST or CLOSED"
        for stock in tickerList:
            if len(stock) > 5:  #Option designation
                underlying = stock[:3]
                #Get Last Quote#
                data = yf.Ticker(underlying)
                lastprice = data.history().tail(1)['Close'].iloc[0]
                #Get Out-of-the-money Call
                callStrike = np.around((lastprice + 5)/5, decimals=0)*5
                #Get Out-of-the-money Put
                putStrike = np.around((lastprice - 5)/5, decimals=0)*5
                today = datetime.now().strftime("%Y-%m-%d")
                expirations = sorted(data.options)
                targetExpiration = None
                if expirations[0] != today:
                    targetExpiration = expirations[0]  #Choose an expiration that is not today#
                else:
                    targetExpiration = expirations[1]  #Choose an expiration that is not today#
                targetExpiration = datetime.strptime(targetExpiration, '%Y-%m-%d')
                #print("Call Strike: %f"%callStrike)
                #print("Put Strike: %f"%putStrike)
                #print("Target Exp: %s"%targetExpiration)
                callSymbol = underlying + targetExpiration.strftime("%y%m%d") + "C" + "%05d000"%callStrike
                putSymbol = underlying + targetExpiration.strftime("%y%m%d") + "P" + "%05d000"%putStrike
                print("CallSymbol: %s"%callSymbol)
                print("PutSymbol: %s"%putSymbol)
                tickerList = [callSymbol,putSymbol]
                marketState = self.getTickerDataFromURL(underlying)["marketState"]
                break
                
            #yFinanceList.append(yf.Ticker(stock))
            #tradeable = yFinanceList[0].info["tradeable"]
            marketState = self.getTickerDataFromURL(stock)["marketState"]
        
        starttime=time.time()
        SumOfActivity = []
        while(marketState==RUNPERIOD):  ##REGULAR/POST##
            while str(datetime.now()).split(' ')[1].split('.')[0].split(':')[-1] != "0%s"%offset:  #SYNC ON 00 seconds##
                time.sleep(.01)
            PrintToScreen = ""
            minuteVolumeCompareList = []
            index = 0
            stockDataList = []
            Error = True
            while Error:  #DATA GATHERING LOOP TO HANDLE LINK FAILS#
                stockDataList = []
                #for stock in yFinanceList:
                for stock in tickerList:
                    try:
                        #stockDataList.append(stock.info)
                        stockDataList.append(self.getTickerDataFromURL(stock))
                        Error = False
                    except Exception as detail:
                        print(detail)
                        Error = True
                        time.sleep(1)
                        break
            for stockData in stockDataList:
                symbol = stockData["symbol"]
                dayOpen = stockData["regularMarketOpen"]
                dayLow = stockData["regularMarketDayLow"]
                dayHigh = stockData["regularMarketDayHigh"]
                if symbol in lastVolume:
                    if (stockData["regularMarketVolume"] - lastVolume[symbol]) > 0:  #I think this is causing bogus values to show up in my report.  How are we getting negative values in the middle of the day?
                        minutevolume = stockData["regularMarketVolume"] - lastVolume[symbol]
                    else:
                        minutevolume = stockData["regularMarketVolume"]
                else:
                    minutevolume = stockData["regularMarketVolume"]
                lastVolume[symbol] = stockData["regularMarketVolume"]
                price = stockData["regularMarketPrice"]
                #tradeable = stockData["tradeable"]
                if len(symbol) > 5:
                    marketState = self.getTickerDataFromURL(symbol[:3])["marketState"]
                else:
                    marketState = self.getTickerDataFromURL(symbol)["marketState"]
                if len(minuteVolumeCompareList) == len(tickerList):
                    minuteVolumeCompareList[index] = price*minutevolume
                else:
                    minuteVolumeCompareList.append(price*minutevolume)
                if len(SumOfActivity)==len(tickerList):
                    SumOfActivity[index]+=price*minutevolume
                else:
                    SumOfActivity.append(price*minutevolume)
                filename = symbol
                sData = {}
                if len(filename) > 5:  #If we are dealing with options, use new format
                    filename = filename[:3]+filename[9]
                    underlyingdata = yf.Ticker(underlying)
                    underlyingprice = data.history().tail(1)['Close'].iloc[0]
                    sData[datetime.now().strftime("%Y-%m-%d %H:%M")] = {"Volume": "%d"%minutevolume, 
                                                                    "Price": "${:,.2f}".format(price),
                                                                    "DayOpen": "${:,.2f}".format(dayOpen),
                                                                    "DayLow": "${:,.2f}".format(dayLow),
                                                                    "DayHigh": "${:,.2f}".format(dayHigh),
                                                                    "Cash": "${:,.2f}".format(price*minutevolume),
                                                                    "Underlying": "${:,.2f}".format(underlyingprice)}
                    sData = json.dumps(sorted(sData)) + ",\n"
                else:   #Old Style Saved#
                    sData = (str(datetime.now()).split(' ')[0] + 
                        " Volume: %7d"%minutevolume +
                        " price: ${:,.2f}".format(price) +
                        " DayOpen: ${:,.2f}".format(dayOpen) +
                        " DayLow: ${:,.2f}".format(dayLow) +
                        " DayHigh: ${:,.2f}".format(dayHigh) +
                        " Cash: ${:13,.2f}".format(price*minutevolume) +
                        " Time " + str(datetime.now()).split(' ')[1].split('.')[0] +
                        "\n" 
                    
                    )
                with open(os.path.join(head,"%s_minute_Data.txt"%filename),"a") as fh:
                    fh.write(sData)
                    fh.flush()
                    PrintToScreen +="%s:"%symbol + " ${:13,.2f}".format(price*minutevolume)
                    PrintToScreen +=" price: ${:6,.2f} ".format(price)
                    PrintToScreen +=" TODAY: ${:13,.2f}  \n".format(SumOfActivity[index])
                index += 1
            if minuteVolumeCompareList[0] > minuteVolumeCompareList[1]:
                PrintToScreen += "       MINUTE BULL                               "
            else:
                PrintToScreen += "       MINUTE BEAR                               "
            if SumOfActivity[0] > SumOfActivity[1]:
                PrintToScreen += "   DAY BULL        "
            else:
                PrintToScreen += "   DAY BEAR        "
            PrintToScreen += "Time " + str(datetime.now()).split(' ')[1].split('.')[0]
            print(PrintToScreen)
            time.sleep(1) #Verify that at least 1 second has passed so it doesn't run multiple times in a second

    '''
    sTicker = Any publically traded stock symbol
    sInterval = 1min, 5min, 15min, 30min, 60min
    sOutputSize = compact,full
    sFormat = pandas,json
    '''
    def getLiveTickerIntraday_values(self, sTicker, sInterval, sOutputSize='compact', sFormat='json', timeout=120, trigger=None):
        ts = TimeSeries(key=os.environ['API_KEY'], output_format=sFormat)
        volume = -1
        SLEEPTIME = 70
        data = None
        meta_data = None
        if sTicker in ["^DJIA"]:  #These indexes don't seem to return data in the first minute.
            timeout = 10
        while(data == None and timeout >= 0):
            try:
                data, meta_data = ts.get_intraday(symbol=sTicker,interval=sInterval,outputsize=sOutputSize)
                print("Data received")
            except Exception as detail:
                print(traceback.print_exc(file=sys.stdout))
                print("Exception thrown getting data from alpha_vantage using trigger %s; Sleeping %d seconds then trying again"%(trigger,SLEEPTIME))
                print("Timeout: %d"%timeout)
                time.sleep(SLEEPTIME)
                timeout = timeout - SLEEPTIME
                data = None
        #if volume <=0:
        #    print("Hit Timeout without achieving valid data; returning with last query")
        return data, meta_data

    def getTickerDataFromURL(self, ticker):
        response_json = None
        attempts = 1
        while((response_json == None) and (attempts <= 120)):
            try:
                myfile = None
                link = "https://query2.finance.yahoo.com/v7/finance/quote?symbols=%s"%ticker
                if six.PY2:
                    f = urllib.urlopen(link)
                    myfile = f.read()
                    response_json = json.loads(myfile)
                if six.PY3:
                    response = urlopen(link)
                    response_json = json.loads(response.read())
                    #print("Looking up ticker %s"%ticker)
                    #print(myfile)
                #response_json = json.loads(myfile)
            except Exception as detail:
                print(detail)
                print(traceback.print_exc(file=sys.stdout))
                time.sleep(2)
                print("Exception thrown getting marketState from web query; attempt %d"%attempts)
                attempts = attempts + 1
                response_json = None
        if response_json:
            return response_json['quoteResponse']['result'][0]
        else:
            print("Forcing termination from marketstate because we can't get a valid json object from Yahoo Finance url")
            return "POST"  #Hard-code to cause script to terminate

    def get_screener(self,table):
        attempts = 1
        MAX_ATTEMPTS = 3
        Success = False
        tables = None
        while ((attempts <= MAX_ATTEMPTS) and (not Success)):
            try:
                #screen = requests.get("https://finviz.com/screener.ashx?v=%s&f=geo_usa,sh_price_u20,ta_change_u20&ft=4&ar=180"%table, headers = headers).text
                screen = requests.get("https://finviz.com/screener.ashx?v=%s&f=geo_usa,sh_curvol_o500,ta_change_u20&ft=4&ar=180"%table, headers = headers).text
                tables = pd.read_html(screen)
                tables = tables[-2]
                tables.columns = tables.iloc[0]
                tables = tables[1:]
                tables["HOD"] = 0
                tables["tHOD"] = DEFAULT_THOD
                tables["dHOD"] = .00
                tables["Float"] = "-1M"
                tables["Rank"] = 0
                tables["Buy"] = 0
                tables = tables.drop(columns=['Country','Sector','Company'],axis=1)
                Success = True
            except Exception as detail:
                attempts = attempts + 1
                print(traceback.print_exc(file=sys.stdout))
                print(detail)
                if attempts <= MAX_ATTEMPTS:
                    print("sleeping 30 seconds")
                    time.sleep(30)
                    print("Exception thrown getting finviz data; attempt %d"%attempts)
        if not Success:
            return None
        return tables

    '''
    Pings Alpha_vantage to get buy trigger
    Parameter List:
        bull_etf - etf for investment in bull market
        bear_etf - etf for investment in bear market
        report_size = "compact" or "full" compact should be all that is needed for minute trigger
    '''
    def GetMorningMarketTrigger(self, bull_etf, bear_etf, report_size="full", timeout=60*5):
        SLEEPTIME = 10
        bull_volume = -1
        bull_close = 0
        bear_volume = -1
        bear_close = 0
        #print "a"
        while ((bull_volume <= 0 or bear_volume <= 0) and timeout > 0):
            #print "b"
            if bull_volume <= 0:
                #print "c"
                data,metadata = self.getLiveTickerIntraday_values(bull_etf,"1min",report_size,"json", timeout=90)
                #print "d"
                if self.triggerTime in data:
                    #print "e"
                    #print(data[self.triggerTime])
                    bull_volume = float(data[self.triggerTime]["5. volume"])
                    bull_close = float(data[self.triggerTime]["4. close"])
            if bear_volume <= 0:
                #print "f"
                data,metadata = self.getLiveTickerIntraday_values(bear_etf,"1min",report_size,"json", timeout=90)
                #print "G"
                if self.triggerTime in data:
                    #print "H"
                    #print(data[self.triggerTime])
                    bear_volume = float(data[self.triggerTime]["5. volume"])
                    bear_close = float(data[self.triggerTime]["4. close"])
            #print "I"
            if (bull_volume <= 0 or bear_volume <= 0):
                #print "J"
                timeout -= SLEEPTIME
                time.sleep(SLEEPTIME)
        return {bull_etf:[bull_volume,bull_close], bear_etf:[bear_volume,bear_close]}

    '''
    sMessage = message contents to send to distribution list text.
    '''
    def sendAlertMsg(self, sMessage, email=None, contactList="SUPPORT"):
        try:
            import smtplib
            import ssl
            username = os.environ.get("EMAIL")
            password = os.environ.get("PWD")
            context=ssl.create_default_context()
            server = smtplib.SMTP('smtp-mail.outlook.com',587)
            server.starttls(context=context)
            server.login(username,password)
            if email:
                self.sendMessage(username, email, sMessage, server)
            elif contactList and contactList in self.dContacts:
                sMessage = "%s: %s"%(contactList,sMessage)
                for contact,email in six.iteritems(self.dContacts[contactList]):
                    self.sendMessage(username, email, sMessage, server)
            else:
                sMessage = "%s: %s"%("SUPPORT",sMessage)
                for contact,email in six.iteritems(self.dContacts["SUPPORT"]):
                    self.sendMessage(username, email, sMessage, server)
            server.quit()
        except Exception as detail:
            print(detail)
            print(traceback.format_exc())
            print("Failed to send alert, we need a backup!")
            print("Failed Message: %s"%sMessage)

    def sendMessage(self, username, email, sMessage, server):
        msg = 'Subject: {}\n\n{}'.format("Stock Alert",sMessage)
        server.sendmail(username, email, msg)
        