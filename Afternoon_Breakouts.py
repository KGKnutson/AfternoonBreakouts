import requests
import time 
import ExtDataSources
from datetime import datetime, timedelta
import traceback
import os
import sys
from os.path import exists
from GetStockRank import GetStockRank
import pandas as pd
from xlrd import open_workbook
from xlwt import Workbook
from xlutils.copy import copy
import yfinance as yf
from bs4 import BeautifulSoup
#from polygon import RESTClient
from ib_insync import *
import xml.etree.ElementTree as ET

DEFAULT_THOD = "9:00:00"

class screener(object):
    gsr = None
    contractDict = {}
    symbolIgnoreList = []

    def __init__(self, clientID=1):
        self.gsr = GetStockRank()
        self.ib = IB()
        self.ib.connect(clientId=clientID)
        #self.client = RESTClient()

    def run_IB_top_percent_scanner(self):
        sub = ScannerSubscription(instrument='STK', locationCode='STK.US.MAJOR', scanCode='TOP_PERC_GAIN')
        tagValues = [
            TagValue("changePercAbove", "20"),
            TagValue("volumeAbove", "500000")
            ]
        scanData = self.ib.reqScannerData(sub, [], tagValues)
        return(scanData)

    def get_tHOD_from_IB_top_percent_scanner(self, currentTime=None):
        #initialize
        TopPercentGainers = {}
        end_date          = datetime.now().strftime('%Y%m%d %H:%M:%S')
        if currentTime:
            end_date          = currentTime.strftime('%Y%m%d %H:%M:%S')
        barSizeSetting    = "1 min"
        history           = "1 D"
        
        #pull top gainers and get history
        scanData = self.run_IB_top_percent_scanner()
        for stock in scanData:
            try:
                symbol = stock.contractDetails.contract.symbol
                if symbol not in self.symbolIgnoreList:
                    self.contractDict[symbol] = stock.contractDetails.contract
                    eqtybars = self.ib.reqHistoricalData(stock.contractDetails.contract, endDateTime=end_date, durationStr=history, barSizeSetting=barSizeSetting, whatToShow='TRADES', useRTH=True)
                    df = util.df(eqtybars)
                    dailyHighIdx = df['high'].idxmax()
                    dailyLowIdx  = df['low'].idxmin()
                    print(symbol)
                    #print(df.iloc[dailyHighIdx])
                    tickerDetails = self.getTickerDetails(symbol,stock.contractDetails.contract)
                    TopPercentGainers[symbol] = [tickerDetails["floatShares"],
                                                tickerDetails["regularMarketPrice"],
                                                tickerDetails["previousClose"],
                                                df.iloc[dailyHighIdx]['high'],
                                                df.iloc[dailyLowIdx]['low'],
                                                df.iloc[dailyHighIdx]['date'].to_pydatetime()
                                                ]
            except Exception as detail:
                print(symbol)
                print(detail)
                print(traceback.print_exc(file=sys.stdout))
                print("Ignoring symbol %s"%symbol)
                self.symbolIgnoreList.append(symbol)
        return(TopPercentGainers)

    def get_top_percent_gainers_from_IB(self):
        scanData = self.run_IB_top_percent_scanner()
        TopPercentGainers = {}
        for stock in scanData:
            try:
                symbol = stock.contractDetails.contract.symbol
                if symbol not in self.symbolIgnoreList:
                    self.contractDict[symbol] = stock.contractDetails.contract
                    tickerDetails = self.getTickerDetails(symbol,stock.contractDetails.contract)
                    TopPercentGainers[symbol] = [tickerDetails["floatShares"],tickerDetails["regularMarketPrice"],tickerDetails["previousClose"],tickerDetails["dailyHigh"],tickerDetails["dailyLow"]]
            except Exception as detail:
                print(symbol)
                print(detail)
                print(traceback.print_exc(file=sys.stdout))
                print("Ignoring symbol %s"%symbol)
                self.symbolIgnoreList.append(symbol)
        return(TopPercentGainers)

    def write_to_file(self, ticker, df, timeOfDay):
        print("Updating workbook")
        wb = Workbook()
        ws = None
        r = 0
        marketCap = 0
        volume = 0
        if exists(os.environ["AFTERNOONBOTRACKING"]):
            rb = open_workbook(os.environ["AFTERNOONBOTRACKING"])
            ws = rb.sheet_by_name("Tracking")
            r = ws.nrows
            #r += 1
            wb = copy(rb)
            ws = wb.get_sheet("Tracking")
        else:
            ws = wb.add_sheet('Tracking')
            ws.write(r,0,"Date")
            ws.write(r,1,"Stock")
            ws.write(r,2,"Float")
            ws.write(r,3,"FloatKey")
            ws.write(r,4,"MarketCap")
            ws.write(r,5,"MarketCapKey")
            ws.write(r,6,"Volume")
            ws.write(r,7,"Volume/$B")
            ws.write(r,8,"Volume/$BKey")
            ws.write(r,9,"Init_tHOD")
            ws.write(r,10,"BO_Time")
            ws.write(r,11,"BO_Level")
            ws.write(r,12,"HOD")
            ws.write(r,13,"Close")
            ws.write(r,14,"MaxProfit")
            ws.write(r,15,"CloseProfit")
            ws.write(r,16,"Drawdown")
            ws.write(r,17,"PreviousClose")
            ws.write(r,18,"tClose")
            ws.write(r,19,"Options")
            ws.write(r,20,"Strike")
            r+=1
        #if (df.loc[df['Ticker']==ticker, 'Buy'].values[0] == True):
        if "B" in df.loc[df['Ticker']==ticker, 'Market Cap'].values[0]:
            marketCap = int(float(df.loc[df['Ticker']==ticker, 'Market Cap'].values[0][:-1])*1000000000)
        if "M" in df.loc[df['Ticker']==ticker, 'Market Cap'].values[0]:
            marketCap = int(float(df.loc[df['Ticker']==ticker, 'Market Cap'].values[0][:-1])*1000000)
        if "M" in df.loc[df['Ticker']==ticker, 'Volume'].values[0]:
            volume = int(float(df.loc[df['Ticker']==ticker, 'Volume'].values[0][:-1])*1000000)
        if "B" in df.loc[df['Ticker']==ticker, 'Volume'].values[0]:
            volume = int(float(df.loc[df['Ticker']==ticker, 'Volume'].values[0][:-1])*1000000000)
        ws.write(r,0,str(datetime.now()).split(' ')[0])
        ws.write(r,1,ticker)
        ws.write(r,4,marketCap)
        ws.write(r,6,volume)
        ws.write(r,7,volume*1000000000/marketCap)
        ws.write(r,9,df.loc[df['Ticker']==ticker, 'tHOD'].values[0])
        ws.write(r,10,timeOfDay)
        ws.write(r,11,float(df.loc[df['Ticker']==ticker, 'HOD'].values[0]))

        wb.save(os.environ["AFTERNOONBOTRACKING"])
        print('Added ticker to spreadsheet')

    #def AddTodaysClosingTimeToFile(self,Date,CloseTime):
    #Run this at the market close#
    
    def yfinance_table_update(self, df):
        #Update table with Yahoo Finance Data#
        MINUTES_BETWEEN_HOD = 45
        timeOfDay = str(datetime.now()).split(' ')[1].split('.')[0]
        marketState = "REGULAR"
        for ticker in df['Ticker']:
            #Do Yahoo lookup for current HOD
            tickerData = extDataSources.getTickerDataFromURL(ticker)
            HOD = df.loc[df['Ticker']==ticker, 'HOD'].values[0]
            ###THIS IS BECAUSE YAHOO FINANCE SOMETIMES GLITCHES HOLDING HOD STEADY SO IT IS ONLY VALID IF IT IS >= PREVIOUS HIGH OF DAY RECORDED
            if float(tickerData["regularMarketDayHigh"]) >= HOD:
                HOD_Distance = (float(tickerData["regularMarketDayHigh"]) - float(tickerData["regularMarketPrice"]))/float(tickerData["regularMarketDayHigh"])*100
            else:
                HOD_Distance = (float(HOD) - float(tickerData["regularMarketPrice"]))/float(HOD)*100
            HOD_Distance = HOD_Distance
    
            #GainersDict[ticker] = [timeOfDay, float(tickerData["regularMarketDayHigh"]), float(tickerData["regularMarketPrice"]), tickerData["regularMarketVolume"], HOD_Distance]
            df.loc[df['Ticker']==ticker,'Price']  = str(tickerData["regularMarketPrice"])  #Update Pandas Table Price
            df.loc[df['Ticker']==ticker,'Volume'] = str(round(float(tickerData["regularMarketVolume"])/1000000,1))+"M"
            df.loc[df['Ticker']==ticker,'Change'] = "%.2f%%"%float(tickerData["regularMarketChangePercent"])  #Update Pandas Table Change
            df.loc[df['Ticker']==ticker,'dHOD']   = float(HOD_Distance)  #Update Pandas Table Change

            #Update Market Cap#
            stockfloat = df.loc[df['Ticker']==ticker, 'Float'].values[0]
            stockfloat = self.convert_shorthand_to_Number(stockfloat)
            MarketCap = int(stockfloat*float(tickerData["regularMarketPrice"]))
            df.loc[df['Ticker']==ticker,'Market Cap'] = str(round(MarketCap/1000000,1))+"M"
            
            if float(tickerData["regularMarketDayHigh"]) > HOD:   #If HOD is above old HOD
                if df.loc[df['Ticker']==ticker, 'tHOD'].values[0] != DEFAULT_THOD:  #Check to see if this is the initial set or an actual breach of HOD.
                    tHOD = datetime.strptime(df.loc[df['Ticker']==ticker, 'tHOD'].values[0],'%H:%M:%S')
                    minutesSincePreviousHigh = int((datetime.strptime(timeOfDay,'%H:%M:%S') - tHOD).total_seconds()/60)
                    if(minutesSincePreviousHigh>=MINUTES_BETWEEN_HOD and not int(df.loc[df['Ticker']==ticker, 'Buy'].values[0])): #1 Hour Consolidation in place and alert not sent#
                        #Make sure the stock isn't an acquisition. They have a very tight range.
                        if (float(tickerData["regularMarketDayHigh"])-float(tickerData["regularMarketDayLow"]))/float(tickerData["regularMarketDayLow"]) <.05:
                            print("Probably an acquisition... < 5%difference from day low to day high")
                        else:
                            df.loc[df['Ticker']==ticker,'Buy'] = True
                            #extDataSources.sendAlertMsg("Buy %s at $%.2f"%(ticker,df.loc[df['Ticker']==ticker, 'HOD'].values[0])) #Turned off alerts for now
                            #Save Stock To Separate List of in play stocks#
                            self.write_to_file(ticker, df, timeOfDay)
                df.loc[df['Ticker']==ticker,'HOD'] = float(tickerData["regularMarketDayHigh"])
                df.loc[df['Ticker']==ticker,'tHOD'] = timeOfDay
            marketState = tickerData["marketState"]
        df = self.add_rank_to_table(df)
        df = df.sort_values(by='dHOD',ascending='True')
        output = df.to_string(formatters={'dHOD': '{:,.2f}%'.format})
        print(output)
        print("")
        return df,marketState

    def convert_shorthand_to_Number(self, shorthand_number):
        if "M" in shorthand_number:
            return int(float(shorthand_number[:-1])*1000000)
        elif "B" in shorthand_number:
            return int(float(shorthand_number[:-1])*1000000000)
        else:
            return int(shorthand_number)

    def add_rank_to_table(self, df):
        SMALL_FLOAT_THRESHOLD = 30000000
        BIG_CAP_THRESHOLD = 100000000
        rank = 0
        for ticker in df['Ticker']:
            marketCap = df.loc[df['Ticker']==ticker, 'Market Cap'].values[0]
            float = df.loc[df['Ticker']==ticker, 'Float'].values[0]
            #Convert shorthand to integer
            marketCap = self.convert_shorthand_to_Number(marketCap)
            float = self.convert_shorthand_to_Number(float)
            df.loc[df['Ticker']==ticker,'Rank'] = int(self.gsr.getStockRank(float,marketCap))
        return df

    def getTickerDetails(self, ticker, contract=None):
        if contract is None:
            if ticker in self.contractDict.keys():
                contract = self.contractDict[ticker]
            else:
                contract = Stock(symbol=ticker, exchange='SMART', currency='USD')
        fundamentals = self.ib.reqFundamentalData(contract, 'ReportSnapshot', fundamentalDataOptions=[])
        #print(fundamentals)
        root = None
        try:
            root = ET.fromstring(fundamentals)
        except Exception as details:
            print(fundamentals)
            raise Exception('invalid fundamentals') from details
            
        tickerDetails = {"floatShares":0}
        
        for child1 in root:
            if child1.tag=="CoGeneralInfo":
                for child2 in child1:
                    if child2.tag=="SharesOut":
                        floatShares = int(float(child2.attrib['TotalFloat']))
                        #print(floatShares)
                        tickerDetails["floatShares"] = floatShares
        ticker = self.ib.reqMktData(contract)
        self.ib.sleep(1)
        regularMarketPrice = ticker.last
        PreviousClose = ticker.close
        tickerDetails["regularMarketPrice"] = regularMarketPrice
        tickerDetails["previousClose"]      = PreviousClose
        tickerDetails["dailyHigh"]          = ticker.high
        tickerDetails["dailyLow"]           = ticker.low
        #print(floatShares, regularMarketPrice, PreviousClose)
        return(tickerDetails)

if __name__ == "__main__":
    screener = screener()
    extDataSources = ExtDataSources.ExtDataSources()
    tables111 = None
    marketState = "REGULAR"
    ExceptionTickers = []
    while tables111 is None and marketState == "REGULAR":
        tables111 = extDataSources.get_screener('111')  #Keep running until we get some data from finviz
        try:
            tickerData = extDataSources.getTickerDataFromURL("SPY")  #Try to get updated MarketState
            marketState = tickerData["marketState"]
        except Exception as detail:
            print("Failed getting market status from yahoo finance")
            marketState = "REGULAR"                                  #JUST KEEP LOOPING UNTIL WE DON"T GET AN EXCEPTION
            print(detail)
            print(traceback.print_exc(file=sys.stdout))
    #tables161 = extDataSources.get_screener('161')
    #tables121 = extDataSources.get_screener('121')
    
    #consolidatedtables = pd.merge(tables111,tables161,how='outer',left_on='Ticker',right_on='Ticker')
    #consolidatedtables = pd.merge(consolidatedtables,tables121,how='outer',left_on='Ticker',right_on='Ticker')
    #consolidatedtables.to_csv('test.csv')
    #print(consolidatedtables)
    #print(tables111)

    if tables111 is not None:
        for ticker in tables111['Ticker']:      #Add Float to columns
            try:
                print("Adding float for %s"%ticker)
                #YahooInfo = yf.Ticker(ticker)
                #PolygonInfo = screener.client.get_ticker_details(ticker)
                #floatShares = float(YahooInfo.info["floatShares"])
                #InitialMarketCap = int(floatShares*float(YahooInfo.info["regularMarketPrice"]))
                #floatShares = float(PolygonInfo.weighted_shares_outstanding)
                tickerDetails = screener.getTickerDetails(ticker)
                floatShares = tickerDetails["floatShares"]
                regularMarketPrice = tickerDetails["regularMarketPrice"]
                InitialMarketCap = int(floatShares*float(regularMarketPrice))
                #InitialMarketCap = int(PolygonInfo.market_cap)
                tables111.loc[tables111['Ticker']==ticker,'Float'] = str(round(floatShares/1000000,1))+"M"
                tables111.loc[tables111['Ticker']==ticker,'Market Cap'] = str(round(InitialMarketCap/1000000,1))+"M"
            except Exception as detail:
                print("Exception thrown trying to add Float data")
                print(detail)
                print(traceback.print_exc(file=sys.stdout))
                print("Dropping Ticker %s"%ticker)
                tables111.drop(tables111[tables111['Ticker']==ticker].index, inplace = True)
                ExceptionTickers.append(ticker)
                #tables111.loc[tables111['Ticker']==ticker,'Float'] = "-1M"
    
        YahooTableUpdateComplete = False
        while not YahooTableUpdateComplete:
            try:
                df,marketState = screener.yfinance_table_update(tables111)  #Update table data using Yahoo
                YahooTableUpdateComplete = True
            except Exception as detail:
                print("Exception thrown trying to update initial finviz table with yahoo data; Will keep retrying")
                print(traceback.print_exc(file=sys.stdout))
                time.sleep(5)
        
        while(marketState=="REGULAR"):
            try:
                print("Sleeping 30 seconds")
                time.sleep(30)
                tables111 = extDataSources.get_screener('111')
                if tables111 is not None:
                    for ticker in tables111['Ticker']:
                        #YahooInfo = yf.Ticker(ticker)
                        #PolygonInfo = screener.client.get_ticker_details(ticker)
                        if len(ticker)==5 and ticker[4]=="W":
                            print("Skipping ticker %s because it is a warrant"%ticker)
                        elif "Exchange Traded Fund" in tables111.loc[tables111['Ticker']==ticker,'Industry'].values[0]:
                            print("Skipping ticker %s because it is an ETF"%ticker)
                        elif ticker in ExceptionTickers:
                            print("Skipping ticker %s because it previously threw an exception trying to get float or price"%ticker)
                        elif ticker not in df['Ticker'].tolist():
                            try:
                                #YahooFloat = float(YahooInfo.info["floatShares"])
                                #YahooFloat = float(PolygonInfo.weighted_shares_outstanding)
                                tickerDetails = screener.getTickerDetails(ticker)
                                YahooFloat = tickerDetails["floatShares"]
                                regularMarketPrice = tickerDetails["regularMarketPrice"]
                                InitialMarketCap = int(YahooFloat*float(regularMarketPrice))
                                print("append new ticker %s to df table"%ticker)
                                print(tables111.loc[tables111['Ticker']==ticker])
                                df = pd.concat([df, tables111.loc[tables111['Ticker']==ticker]])
                                df.loc[df['Ticker']==ticker,'Float'] = str(round(YahooFloat/1000000,1))+"M"
                                InitialMarketCap = int(YahooFloat*float(regularMarketPrice))
                                df.loc[df['Ticker']==ticker,'Market Cap'] = str(round(InitialMarketCap/1000000,1))+"M"
                            except Exception as detail:
                                print(detail)
                                print(traceback.print_exc(file=sys.stdout))
                                print("Skipping ticker %s because Yahoo Finance is missing Float"%ticker)
                                ExceptionTickers.append(ticker)
                else:
                    print("Failed to get Finviz Data during last cycle")
                df,marketState = screener.yfinance_table_update(df)
            except Exception as detail:
                print(traceback.print_exc(file=sys.stdout))
                print("Exception thrown in minute loop: %s"%detail)
                try:
                    tickerData = extDataSources.getTickerDataFromURL("SPY")  #Try to get updated MarketState
                    marketState = tickerData["marketState"]
                except Exception as detail:
                    print("Failed getting market status from yahoo finance")
                    marketState = "REGULAR"                                  #JUST KEEP LOOPING UNTIL WE DON"T GET AN EXCEPTION
                    print(detail)
                    print(traceback.print_exc(file=sys.stdout))
    else:
        print("Did not run loop as we did not have an initial finviz table to work with and market now closed.")

            
            
    