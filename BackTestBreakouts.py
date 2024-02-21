from datetime import *
from line_profiler import LineProfiler
from ib_insync import *
import pytz
import pandas_market_calendars as mcal
import os
import numpy as np
import traceback
import pandas as pd
import StockProps
import decimal
import statistics

class BackTestBreakouts(object):
    TWO_SIGNAL_VOLUME = {}
    MIN_DAILY_PRICE = {}
    MAX_DAILY_PRICE = {}
    MAX_DAILY_VOLUME = {}
    DAILY_OPEN_PRICE = {}
    VOLUME_TRACKER = {}
    PILO_ALERTS_DICT = {}
    FIRST_MINUTE_DATA_OFFSET = {}
    PREFERRED_OPTION = None
    STRATEGY = None
    OVERNIGHT = False
    VERBOSE = True
    STOP_SLIPPAGE = 0.03 #Assume 3 percent slippage for every STOP usage
    Sum_of_closes = 0
    overnightExitTime = None
    PolygonData = False
    daysTillExpiration = None
    sameDayAvailable = None

    def __init__(self, strategy, polygonData=False, verbose=False):
        self.STRATEGY = strategy
        self.ib = IB()
        self.ib.connect(clientId=100)   #Hopefully we never get up to 100 connections otherwise this will need to change
        if polygonData:
            self.PolygonData = True
        self.op = StockProps.StockProps()
        self.df = {}
        print("Strategy: %s"%strategy)
        self.VERBOSE = verbose

    def main(self, cycle=None, chunk=None, interval=None):
        if self.VERBOSE:
            self.observationFile = open("%s_Analysis.csv"%(self.STRATEGY), "w")
            lineToWrite = "Date,P/C,Grp,MktGap,Open,High,Low,Trigger,Time,Loss2Profit,MaxProfit,%s,Volume,AvgVolume,Trades,AvgTrades,High,AvgHigh,Low,AvgLow,PostVolumeAvg,PostTradesAvg,daysTillExpiration,sameDayAvailable\n"%self.STRATEGY
            self.observationFile.write(lineToWrite)
            self.observationFile.flush()
        start_time = datetime.now()
        DATE       = 0
        SYMBOL     = 1
        LMTPRICE   = 2
        RANK       = 3
        PREVCLOSE  = 4
        ##########Determine Operation Window to Speed up looping##################
        tempProps = self.op.getTriggerProperties(self.STRATEGY.upper(), 0)
        EARLIEST_BUY_TIME = tempProps.get("EARLIEST_BUY_TIME")
        LAST_BUY_TIME = tempProps.get("LAST_BUY_TIME")
        if EARLIEST_BUY_TIME:
            EARLIEST_BUY_TIME = datetime.strptime(EARLIEST_BUY_TIME,"%H:%M:%S")
        if LAST_BUY_TIME:
            LAST_BUY_TIME = datetime.strptime(LAST_BUY_TIME,"%H:%M:%S")
        ##########Determine Operation Window to Speed up looping##################
        ANALYSIS_START_DATE = datetime.now().replace(year=2023, month=4,day=27,hour=6,minute=30,second=00,microsecond=00)
        ANALYSIS_END_DATE = datetime.now() + timedelta(days=10)
        #ANALYSIS_END_DATE = ANALYSIS_START_DATE + timedelta(days=5)  #For forcing backtester to a particular date range
        nyse = mcal.get_calendar('NYSE')
        marketHours = nyse.schedule(ANALYSIS_START_DATE.strftime('%Y-%m-%d'),(ANALYSIS_END_DATE + timedelta(days=10)).strftime('%Y-%m-%d'))
        marketWeeklySchedule = []
        for row in marketHours.iterrows():
            marketWeeklySchedule.append(row[0].strftime("%Y%m%d"))
        ADJUSTTRIGGERS = [1]
        ADJUSTTRGT = [-21]
        chunks = [(-10,100,10),(100,200,10),(200,300,10)]
        if cycle:
            if chunk:
                chunkTuple = chunks[int(chunk)]
                ADJUSTTRGT = range(chunkTuple[0],chunkTuple[1],chunkTuple[2])    #Break into chunks
            else:
                ADJUSTTRGT = range(chunks[0][0],chunks[2][1],chunks[2][2])  #Run as a single loop for all desired data
            ADJUSTTRIGGERS = range(500,2500,500)
        for adjusttrigger in ADJUSTTRIGGERS:
            #adjusttrigger = adjusttrigger/1000
            for adjusttarget in ADJUSTTRGT:
                adjusttarget = adjusttarget/100
                if True: #not cycle or adjusttarget < adjusttrigger:
                    TextFile = "%s\OptionTraderBarData\BreakoutAlerts.csv"%(os.getenv('OPTIONTRADERALGOPATH'))   #New google shared path
                    #if self.PolygonData:
                    #    TextFile = "%s\%s\Polygon\%s.csv"%(os.getenv('OPTIONTRADERBARDATA'),underyling,underlying)   #New google shared path
                    #print(TextFile)
                    TotalProfit = 0
                    optionTypeProfit = {"C":0,"P":0}
                    monthlyReturns = {}
                    wins = 0
                    losses = 0
                    self.Sum_of_closes = 0
                    market_close = 0
                    mrkt_gap = 0.0
                    last_date = None
                    with open(TextFile, "r") as rd:
                        line = None
                        for line in rd:
                            #This condition will skip the first line.
                            if "breakoutTime" in line:
                                pass
                            else:
                                self.TWO_SIGNAL_VOLUME = {} #resetting this every day.
                                self.MIN_DAILY_PRICE = {}   #resetting this every day.
                                self.MAX_DAILY_PRICE = {}   #resetting this every day.
                                self.VOLUME_TRACKER = {}    #resetting this every day.
                                self.MAX_DAILY_VOLUME = {}  #resetting this every day.
                                self.DAILY_OPEN_PRICE = {}  #resetting this every day.
                                self.FIRST_MINUTE_DATA_OFFSET = {} #resetting this every day.
                                pilo_alerts_dict      = {}  #resetting this every day.
                                linedata = line.split(",")
                                date_time = linedata[DATE]
                                date = datetime.strptime(date_time,"%Y_%m_%d_%H:%M:%S")
                                underlying = linedata[SYMBOL]
                                prevClose  = linedata[PREVCLOSE]
                                boPrice    = linedata[LMTPRICE]
                                morningOpen = date.replace(hour=6, minute=30, second=00, microsecond=00)  #Make sure that this data is saved
                                todaysMarketHourIndex = marketWeeklySchedule.index(morningOpen.strftime("%Y%m%d"))
                                nextTradeDay = marketWeeklySchedule[todaysMarketHourIndex + 1]
                                nextTradeDate = datetime.strptime(nextTradeDay,"%Y%m%d")                                
                                if morningOpen >= ANALYSIS_START_DATE and morningOpen <= ANALYSIS_END_DATE:
                                    #GetPutCallDatasets
                                    offset = 0  #Start with no offset
                                    #print()
                                    print("%s BO Price: %s"%(underlying,float(boPrice)))
                                    print(self.getStockDataFromCSV(underlying, date, nextTradeDate=nextTradeDate))
                        if (wins+losses) > 0:
                            print("\nTotal Return: %.2f%%   Win Rate: %.2f%%"%(TotalProfit, wins*100/(wins+losses)))
                        print("Sum of closes: %.2f%%"%self.Sum_of_closes)
                        print("PutCallSums: %s"%optionTypeProfit)
                        for month,profit in monthlyReturns.items():
                            print("%s: %.2f%%"%(month.rjust(20),profit))
                        if self.VERBOSE:
                            self.observationFile.close()
                        if cycle:
                            try:
                                with open("%s_EarlyExitsVolumeBased.txt"%underlying, "a") as w:
                                    w.write("Trigger:%.2f%%,MinTarget:%.2f%%,Total Return:%.2f%%,Puts:%.2f%%,Calls:%.2f%%,Win Rate:%.2f%%\n"%(adjusttrigger*100,adjusttarget*100,TotalProfit,optionTypeProfit["P"],optionTypeProfit["C"],wins*100/(wins+losses)))
                            except Exception as detail:
                                print(detail)
                                time.sleep(5)
                                with open("TrailCompares.txt", "a") as w:
                                    w.write("Trigger: %.2f%%  Target: %.2f%%  Total Return: %.2f%%   Win Rate: %.2f%%\n"%(adjusttrigger*100,adjusttarget*100,TotalProfit, wins*100/(wins+losses)))
        end_time = datetime.now()
        run_time = end_time - start_time
        print("Elapsed Time: %ds"%run_time.seconds)
        print("Strategy:%s; Interval:%s; Polygon:%r; Overnight:%r; %s"%(self.STRATEGY,interval,self.PolygonData,self.OVERNIGHT,datetime.now()))





    def getStockDataFromCSV(self, underlying, date, nextTradeDate=None, interval=None):
        EquityContract = Stock(symbol=underlying, exchange='SMART', currency='USD')
        self.ib.qualifyContracts(EquityContract)
        Filename = "%s.csv"%(underlying)
        #if interval and interval != "1min":
        #    Filename = "%s_%s.csv"%(underlying,interval)
        Filepath = "%s"%(os.getenv('AFTERNOONBODATA'))  #New Google Drive Share Path
        #if self.PolygonData:
        #    Filepath = "%s\%s\Polygon"%(os.getenv('OPTIONTRADERBARDATA'),underlying)  #New Google Drive Share Path
        Filepath = os.path.join(Filepath,Filename)
        #print(Filepath)
        #print(" ")
        if os.path.exists(Filepath):
            df = pd.read_csv(Filepath)
            #date = datetime(int(date.split("-")[0]), int(date.split("-")[1]), int(date.split("-")[2])).date()
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
            filtered_df = None
            if self.OVERNIGHT:
                df = df.set_index('date')
                df.index = pd.to_datetime(df.index)
                filtered_df = df.sort_index().loc[date:nextTradeDate.strftime("%Y-%m-%d")].reset_index()
            else:
                filtered_df = df.loc[df['date'].dt.strftime('%Y-%m-%d') == date].reset_index()
            #print(filtered_df)
            if not filtered_df.empty():
                return(localSymbol,filtered_df)

        #If we didn't find valid data in the local file, pull it from IB and update the file
        try:
            barSizeSetting = "1 min"
            history        = "3 D"
            end_date = date.strftime('%Y%m%d %H:%M:%S')
            if nextTradeDate:
                end_date = nextTradeDate.strftime('%Y%m%d %H:%M:%S')
            bars = self.ib.reqHistoricalData(EquityContract, endDateTime=end_date, durationStr=history, barSizeSetting=barSizeSetting, whatToShow='TRADES', useRTH=True)
            df = util.df(bars)
            #self.saveDFToFile(underlying, df)
            return(underlying, df)
        except Exception as detail:
            print(detail)
            print(traceback.format_exc())
        print("Filepath doesn't exist: %s"%Filepath)
        return(None,None)

    def saveDFToFile(self, symbol, df, intervalModifier=""):
        fileName = "%s.csv"%symbol
        if intervalModifier != "":
            fileName = "%s%s.csv"%(symbol,intervalModifier)
        backupFile = "%s%s"%(os.getenv('AFTERNOONBODATA'),fileName)
        for filepath in [backupFile]:
            row = 0 
            LastEntryDateTime = None
            ExistingFile = False
            lastdate = None
            if not df.empty:
                foldername = os.path.dirname(filepath)
                if not os.path.exists(foldername):           #Make the folder path if it doesn't exist#
                    os.makedirs(foldername)
                if os.path.exists(filepath): #Read Date/time 
                    ExistingFile = True
                    with open(filepath, "r") as rd:
                        line = None
                        for line in rd:
                            pass
                        last_line = line
                        #Date Format 2021-11-10 08:18:00 #
                        date_time = last_line.split(",")[0]
                        #print(date_time)
                        if len(date_time.split(" "))==1:  #Row has date only
                            lastdate = datetime.datetime(int(date_time.split("-")[0]), int(date_time.split("-")[1]), int(date_time.split("-")[2])).date()
                        else:  #Row has date and time
                            date = date_time.split(" ")[0]
                            time = date_time.split(" ")[1]
                            lastdate = datetime.datetime(int(date.split("-")[0]), 
                                                        int(date.split("-")[1]), 
                                                        int(date.split("-")[2]),
                                                        int(time.split(":")[0]), 
                                                        int(time.split(":")[1]), 
                                                        int(time.split(":")[2]),)
                        #print(lastdate)
                if lastdate:
                    dfToWrite = df[df['date'] > lastdate]
                    #df.loc[(df.index > start) & (df.index < end), 'C'] = 100
                else:
                    dfToWrite = df
                if not dfToWrite.empty:
                    #print(dfToWrite.to_csv())
                    with open(filepath, 'a+') as f:
                        if ExistingFile:
                            #print(dfToWrite)  #Validate this looks correct before we write to existing file#
                            f.write(dfToWrite.to_csv(header=False, index=False, line_terminator='\n'))
                        else:
                            f.write(dfToWrite.to_csv(header=True, index=False, line_terminator='\n'))
        

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-verbose", "--verbose", help="print additional information for research", action="store_true")
    parser.add_argument("-polygon", "--polygon", help="Use Polygon.io Historical Data", action="store_true")
    parser.add_argument("-cycle", "--cycle", help="cycle through variables and save data", action="store_true")
    parser.add_argument("-chunk", "--chunk", help="Used in combination with cycle to break computation into 3 chunk sizes")
    parser.add_argument("-strategy", "--strategy", help="Can force a different strategy to run besides default.")
    parser.add_argument("-interval", "--interval", help="Pass in the desired interval with which to operate on.")
    args = parser.parse_args()
    interval = "1min"
    strategy = "afternoonbreakouts"

    if args.strategy:
        strategy = args.strategy
    
    if args.interval:
        interval = args.interval

    backtest = BackTestBreakouts(strategy, args.polygon, args.verbose)
    backtest.main(args.cycle, args.chunk, interval)
    #lp = LineProfiler()
    #lp_wrapper = lp(backtest.main)
    #lp_wrapper(underlying, args.cycle, args.chunk)
    #lp.print_stats()