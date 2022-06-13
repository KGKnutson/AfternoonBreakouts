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

DEFAULT_THOD = "9:00:00"

class screener(object):
    gsr = None

    def __init__(self):
        self.gsr = GetStockRank()

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
                            extDataSources.sendAlertMsg("Buy %s at $%.2f"%(ticker,df.loc[df['Ticker']==ticker, 'HOD'].values[0]))
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
            

if __name__ == "__main__":
    screener = screener()
    extDataSources = ExtDataSources.ExtDataSources()
    tables111 = None
    marketState = "REGULAR"
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
                YahooInfo = yf.Ticker(ticker)
                tables111.loc[tables111['Ticker']==ticker,'Float'] = str(round(float(YahooInfo.info["floatShares"])/1000000,1))+"M"
                InitialMarketCap = int(float(YahooInfo.info["floatShares"])*float(YahooInfo.info["regularMarketPrice"]))
                tables111.loc[tables111['Ticker']==ticker,'Market Cap'] = str(round(InitialMarketCap/1000000,1))+"M"
            except Exception as detail:
                print("Exception thrown trying to add Float data")
                print(detail)
                print("Dropping Ticker %s"%ticker)
                tables111.drop(tables111[tables111['Ticker']==ticker].index, inplace = True)
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
                time.sleep(30)
                tables111 = extDataSources.get_screener('111')
                if tables111 is not None:
                    for ticker in tables111['Ticker']:
                        YahooInfo = yf.Ticker(ticker)
                        if len(ticker)==5 and ticker[4]=="W":
                            print("Skipping ticker %s because it is a warrant"%ticker)
                        elif "Exchange Traded Fund" in tables111.loc[tables111['Ticker']==ticker,'Industry'].values[0]:
                            print("Skipping ticker %s because it is an ETF"%ticker)
                        elif ticker not in df['Ticker'].tolist():
                            if YahooInfo.info["floatShares"]:  #Validate that Float Exists in Yahoo Finance
                                print("append new ticker %s to df table"%ticker)
                                print(tables111.loc[tables111['Ticker']==ticker])
                                df = pd.concat([df, tables111.loc[tables111['Ticker']==ticker]])
                                df.loc[df['Ticker']==ticker,'Float'] = str(round(float(YahooInfo.info["floatShares"])/1000000,1))+"M"
                                InitialMarketCap = int(float(YahooInfo.info["floatShares"])*float(YahooInfo.info["regularMarketPrice"]))
                                df.loc[df['Ticker']==ticker,'Market Cap'] = str(round(InitialMarketCap/1000000,1))+"M"
                            else:
                                print("Skipping ticker %s because Yahoo Finance is missing Float"%ticker)
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

            
            
    