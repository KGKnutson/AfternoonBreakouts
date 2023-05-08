from datetime import *
from line_profiler import LineProfiler
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
    PUTCALLIDX        = 12
    #EARLIESTTRIGGERTIME  = datetime.strptime('6:33', '%H:%M').time()
    TWO_SIGNAL_VOLUME = {}
    MIN_DAILY_PRICE = {}
    MAX_DAILY_PRICE = {}
    MAX_DAILY_VOLUME = {}
    DAILY_OPEN_PRICE = {}
    VOLUME_TRACKER = {}
    PILO_ALERTS_DICT = {}
    FIRST_MINUTE_DATA_OFFSET = {}
    OFFSETDICT = {
                  "QQQ":[-5, 0, 5],
                  "SPY":[-5, 0, 5],
                  "TSLA":[-5, 0, 5],
                  "BA":[-5, 0, 5],
                  "COIN":[-5, 0, 5],
                  "USO":[-1, 0, 1],
                  "XLE":[-1, 0, 1],
                 }
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
    INVERSE_OPTION_DICT = {"P":"C",
                           "C":"P"
                          }

    def __init__(self, strategy, polygonData=False, verbose=False):
        self.STRATEGY = strategy
        if polygonData:
            self.PolygonData = True
        self.op = StockProps.StockProps()
        self.df = {}
        print("Strategy: %s"%strategy)
        self.VERBOSE = verbose

    def main(self, underyling, cycle=None, chunk=None, interval=None):
        if self.VERBOSE:
            self.observationFile = open("%s_%s_Analysis.csv"%(underlying,self.STRATEGY), "w")
            lineToWrite = "Date,P/C,Grp,MktGap,Open,High,Low,Trigger,Time,Loss2Profit,MaxProfit,%s,Volume,AvgVolume,Trades,AvgTrades,High,AvgHigh,Low,AvgLow,PostVolumeAvg,PostTradesAvg,daysTillExpiration,sameDayAvailable\n"%self.STRATEGY
            self.observationFile.write(lineToWrite)
            self.observationFile.flush()
        start_time = datetime.now()
        EXPIRATION = 8
        DATE = 0
        OPEN = 1
        HIGH = 2
        LOW = 3
        CLOSE = 4
        INCLUDESAMEDAYEXPIRATION = self.getSameDayExpirationSetting(underlying)
        ##########Determine Operation Window to Speed up looping##################
        tempProps = self.op.getTriggerProperties(underlying.upper(), 0, "C")
        EARLIEST_CALL_BUY_TIME = tempProps.get("EARLIEST_BUY_TIME")
        LAST_CALL_BUY_TIME = tempProps.get("LAST_BUY_TIME")
        if EARLIEST_CALL_BUY_TIME:
            EARLIEST_CALL_BUY_TIME = datetime.strptime(EARLIEST_CALL_BUY_TIME,"%H:%M:%S")
        if LAST_CALL_BUY_TIME:
            LAST_CALL_BUY_TIME = datetime.strptime(LAST_CALL_BUY_TIME,"%H:%M:%S")
        tempProps = self.op.getTriggerProperties(underlying.upper(), 0, "P")
        EARLIEST_PUT_BUY_TIME = tempProps.get("EARLIEST_BUY_TIME")
        LAST_PUT_BUY_TIME = tempProps.get("LAST_BUY_TIME")
        if EARLIEST_PUT_BUY_TIME:
            EARLIEST_PUT_BUY_TIME = datetime.strptime(EARLIEST_PUT_BUY_TIME,"%H:%M:%S")
        if LAST_PUT_BUY_TIME:
            LAST_PUT_BUY_TIME = datetime.strptime(LAST_PUT_BUY_TIME,"%H:%M:%S")
        EARLIEST_BUY_TIME = EARLIEST_CALL_BUY_TIME
        LAST_BUY_TIME = LAST_CALL_BUY_TIME
        if EARLIEST_BUY_TIME is None or (EARLIEST_PUT_BUY_TIME and EARLIEST_PUT_BUY_TIME < EARLIEST_BUY_TIME):
            EARLIEST_BUY_TIME = EARLIEST_PUT_BUY_TIME
        if LAST_BUY_TIME is None or (LAST_PUT_BUY_TIME and LAST_PUT_BUY_TIME > LAST_BUY_TIME):
            LAST_BUY_TIME = LAST_PUT_BUY_TIME
        ##########Determine Operation Window to Speed up looping##################
        self.OVERNIGHT = self.getOvernightHoldSetting(underlying)
        ANALYSIS_START_DATE = datetime.now().replace(year=2022, month=11,day=1,hour=6,minute=30,second=00,microsecond=00)
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
                    TextFile = "%s\OptionTraderBarData\%s\%s.csv"%(os.getenv('OPTIONTRADERALGOPATH'),underyling,underlying)   #New google shared path
                    if self.PolygonData:
                        TextFile = "%s\%s\Polygon\%s.csv"%(os.getenv('OPTIONTRADERBARDATA'),underyling,underlying)   #New google shared path
                    #print(TextFile)
                    TotalProfit = 0
                    optionTypeProfit = {"C":0,"P":0}
                    monthlyReturns = {}
                    wins = 0
                    losses = 0
                    self.Sum_of_closes = 0
                    market_close = 0
                    mrkt_gap = 0.0
                    with open(TextFile, "r") as rd:
                        line = None
                        for line in rd:
                            if "date" in line:
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
                                morningOpen = datetime.strptime(date_time,'%Y-%m-%d').replace(hour=6, minute=30, second=00, microsecond=00)  #Make sure that this data is saved
                                if morningOpen >= ANALYSIS_START_DATE and morningOpen <= ANALYSIS_END_DATE:
                                    #GetPutCallDatasets
                                    offset = 0  #Start with no offset
                                    print()
                                    print("Underlying Open Price: %s"%float(linedata[OPEN]))
                                    strike = np.around((float(linedata[OPEN]) + offset)/5, decimals=0)*5
                                    if 1 in self.OFFSETDICT[underlying]:
                                        strike = round(float(linedata[OPEN]))      #This line rounds to nearest $1 instead of $5
                                    expiration = linedata[EXPIRATION].strip("\n")
                                    expirationsFromFile = expiration
                                    samedayexpireavailable = False
                                    market_open = float(linedata[OPEN])
                                    if market_close > 0:
                                        mrkt_gap = (market_open - market_close)/market_close
                                    market_close = float(linedata[CLOSE])
                                    if not INCLUDESAMEDAYEXPIRATION:
                                        expiration = expiration.replace("%s"%linedata[DATE].replace("-",""),"")  #Remove Same day expiration
                                        expiration = expiration.replace(";","")
                                    else:
                                        if linedata[DATE].replace("-","") in expiration:                         #Use only same day expiration when available; Use both if commented out
                                            expiration = linedata[DATE].replace("-","")
                                    if expiration != expirationsFromFile:
                                        samedayexpireavailable=True
                                    for expiration in expiration.split(";"):
                                        minuteDataDict = {}
                                        dailyChoice = None
                                        resp = None
                                        #print(minuteDataDict.keys())
                                        DAILYPERFORMANCE = 7
                                        SELLTIME = 8
                                        dailyBuys = 0
                                        DEFAULT_BUYS = 1
                                        MaxBuys = DEFAULT_BUYS
                                        overRideProps = {}
                                        forceFutureOptionType = None
                                        todaysMarketHourIndex = marketWeeklySchedule.index(morningOpen.strftime("%Y%m%d"))
                                        nextTradeDay = marketWeeklySchedule[todaysMarketHourIndex + 1]
                                        nextTradeDate = datetime.strptime(nextTradeDay,"%Y%m%d")
                                        self.daysTillExpiration = marketWeeklySchedule.index(expiration) - todaysMarketHourIndex
                                        self.sameDayAvailable = samedayexpireavailable
                                        marketOpen = datetime.fromtimestamp(marketHours.iloc[todaysMarketHourIndex]['market_open'].timestamp())
                                        marketClose = datetime.fromtimestamp(marketHours.iloc[todaysMarketHourIndex]['market_close'].timestamp())
                                        openMinutes = int((marketClose - marketOpen).seconds/60)
                                        optionDataOffset = 60
                                        piloTriggerOffset = 60
                                        for offset in self.OFFSETDICT[underlying]: #[-5, 0, 5]:
                                            for optionType in ["C", "P"]:
                                                localSymbol, optiondf = self.getOptionDataFromCSV(underlying, expiration, optionType, strike+offset, date_time, nextTradeDate, interval)
                                                if localSymbol:     #Don't filter out contracts if they are missing the first couple minutes of the day.
                                                    minuteDataDict[localSymbol] = optiondf
                                        Pilo_Alerts_TimePeriods = self.getPiloTradeAlertsSetting(underlying)
                                        if "15s" in Pilo_Alerts_TimePeriods:
                                            interval = "15s"
                                            optionDataOffset = 15
                                            piloTriggerOffset = 15
                                        for timePeriod in Pilo_Alerts_TimePeriods:
                                            #print(expiration)
                                            pilo_alerts_dict[timePeriod] = self.getPiloAlertsFromCSV(underlying,timePeriod,morningOpen.date(),nextTradeDate)
                                        #marketOpen = datetime.strptime(date_time,'%Y-%m-%d').replace(hour=6, minute=30, second=00, microsecond=00)
                                        #marketClose = datetime.strptime(date_time,'%Y-%m-%d').replace(hour=13, minute=00, second=00, microsecond=00)
                                        currentTime = marketOpen
                                        if EARLIEST_BUY_TIME:
                                            EARLIEST_BUY_TIME = datetime.combine(currentTime.date(), EARLIEST_BUY_TIME.time())
                                        if LAST_BUY_TIME:
                                            LAST_BUY_TIME = datetime.combine(currentTime.date(), LAST_BUY_TIME.time())
                                        if self.overnightExitTime and self.overnightExitTime > currentTime:
                                            currentTime = self.overnightExitTime - timedelta(minutes=1)
                                            self.overnightExitTime = None
                                        index = int((currentTime - marketOpen).seconds)
                                        print("TRADE DATE: %s; Option Expiration:%s  Overnight Gap:%.2f%% daysTillExpiration:%d sameDayAvailable:%r"%(date_time,expiration,mrkt_gap*100,self.daysTillExpiration,samedayexpireavailable))
                                        while currentTime <= marketClose and currentTime < datetime.now():
                                            buyable = True
                                            if EARLIEST_BUY_TIME and currentTime < EARLIEST_BUY_TIME - timedelta(minutes=1):
                                                buyable = False
                                            elif LAST_BUY_TIME and currentTime > LAST_BUY_TIME:
                                                buyable = False
                                            if dailyBuys < MaxBuys and buyable:
                                                overRideProps["EXPIRATIONDATE"] = expiration
                                                if cycle:
                                                    overRideProps["EARLYEXITTRIGGER"] = adjusttrigger
                                                    overRideProps["EARLYEXITMINIMUM"] = adjusttarget
                                                buyContractKey, HighVoteVolume, HighVoteDollarVolume, triggerPrice, triggerTime, buyProps, piloAlertTime, lastBuyTime = self.checkForBuySignal(minuteDataDict, forceFutureOptionType, currentTime, marketOpen, marketClose, mrkt_gap, dailyBuys, pilo_alerts_dict, overRideProps, piloTriggerOffset, optionDataOffset)
                                                if buyContractKey:
                                                    if "BIG_GAP_SINGLE_BUY" in buyProps and abs(mrkt_gap) >= buyProps["BIG_GAP_SINGLE_BUY"]:
                                                        buyProps["MAXDAILYBUYS"] = 1
                                                    ####NOTE: The code below currently forces an expired put order to be a call after we start looking for triggers again; 
                                                    ####      This protects us from potentially picking up a sell put order as a future trigger given puts have a big stop after the first 75 minutes of the market.
                                                    ####      Calls are unaffected by this because we don't currently raise the MaxBuys for calls so we don't force a future optionType for an expired call order.
                                                    if dailyBuys == 0:  #Check to see if this buy allows subsequent buys
                                                        MaxBuys = buyProps["MAXDAILYBUYS"]
                                                    if "C" == buyContractKey[self.PUTCALLIDX]:           #Force the future option purchase to be the opposite of the initial buy
                                                        forceFutureOptionType = "P"
                                                    else:
                                                        forceFutureOptionType = "C"
                                                    dailyChoice = "%s @ %s based on %d and $%d and at price %.2f "%(buyContractKey,currentTime.strftime('%H:%M:%S'),HighVoteVolume,HighVoteDollarVolume,triggerPrice)
                                                    resp = self.getMaxGainMaxLossFromDF(buyContractKey, minuteDataDict, buyProps, currentTime, dailyBuys, openMinutes, marketClose, triggerPrice, mrkt_gap, pilo_alerts_dict, piloAlertTime, nextTradeDay)
                                                    if resp:
                                                        sellTime = resp[SELLTIME]
                                                        index = int((sellTime - marketOpen).seconds)   #This fast-tracks our loop to just after we sold our position
                                                        if self.OVERNIGHT and sellTime.date() > currentTime.date():          #This is for overnight positions; We are done trading for the day.
                                                            index = openMinutes*60
                                                            self.overnightExitTime = sellTime                                #update loop for the next trade day
                                                        if "PILO_ENTRY" in buyProps:
                                                            #We subtract off 2 minutes because we add 1 minute at the bottom of the loop and we want to re-run the previous period.
                                                            #We rollback a period because alerts happen at the front-end of a window
                                                            if interval == "15s":
                                                                index = index - 30
                                                            elif interval == "30s":
                                                                index = index - 60
                                                            else:
                                                                index = index - 120
                                                            #print("Roll back index so we can pick evaluate the trigger for the minute we just exited the position")
                                                            #print("Current Time: %s"%(marketOpen + timedelta(minutes=index)).strftime("%H:%M:%S"))
                                                        if "PILO_EXITED" in buyProps:
                                                            print("PILO_EXITED @ %s"%sellTime)
                                                        dailyChoice = dailyChoice + "Sold @ %s"%sellTime.strftime('%Y-%m-%d %H:%M:%S')
                                                        TotalProfit = TotalProfit + float(resp[DAILYPERFORMANCE])
                                                        optionTypeProfit[buyContractKey[self.PUTCALLIDX]] = optionTypeProfit[buyContractKey[self.PUTCALLIDX]] + float(resp[DAILYPERFORMANCE])
                                                        month = morningOpen.strftime("%B %Y")
                                                        try:
                                                            monthlyReturns[month] = monthlyReturns[month] + float(resp[DAILYPERFORMANCE])
                                                        except:
                                                            monthlyReturns[month] = float(resp[DAILYPERFORMANCE])
                                                        if resp[DAILYPERFORMANCE] > 0:
                                                            wins = wins + 1
                                                        elif resp[DAILYPERFORMANCE] < 0:
                                                            losses = losses + 1
                                                        print("%s, Daily: %.2f%%"%(resp[0:DAILYPERFORMANCE],resp[DAILYPERFORMANCE]))
                                                    else:
                                                        print("Limit not Triggered")
                                                        forceFutureOptionType = None
                                                        index = index + buyProps["EXPIRATION"]*60  #Able to re-buy after expiration
                                                        dailyChoice = dailyChoice + " EXPIRED after %d minutes"%buyProps["EXPIRATION"]
                                                        print(dailyChoice)
                                                        dailyChoice = None
                                                        if dailyBuys == 0:  #If this was our first buy, reset MaxBuys
                                                            MaxBuys = DEFAULT_BUYS    #Reset MaxBuys
                                                if dailyChoice:
                                                    print("%s"%(dailyChoice))
                                                    resp = None
                                                    dailyChoice = None
                                                    dailyBuys = dailyBuys + 1
                                            if interval in ["30s","15s"]:
                                                if interval=="30s":
                                                    index = index + 30
                                                elif interval=="15s":
                                                    index = index + 15
                                            else:
                                                index = index + 60
                                            currentTime = marketOpen + timedelta(seconds=index)
                                            
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

    def getSameDayExpirationSetting(self, underlying):
        return self.op.getSameDayExpirationSetting(underlying)

    def getPiloTradeAlertsSetting(self, underlying):
        return self.op.getPiloTradeAlertsSetting(underlying)

    def getOvernightHoldSetting(self, underlying):
        return self.op.getOvernightHoldSetting(underlying)

    def getEarliestBuyTimeSetting(self, underlying):
        return self.op.getEarliestBuyTimeSetting(underlying)

    def checkForBuySignal(self, OptionDataDict, reqOptionType, triggerTime, marketOpen, marketClose, mrkt_gap, buyPosition, piloAlertsDict, overRideProps=None, piloTriggerOffset=60, optionDataOffset=60):
        buyContractKey       = None
        HighVoteDollarVolume = None
        HighVoteVolume       = None
        HighVotetriggerPrice = None
        HighVoteLowPrice     = None
        BuySignalprops       = None
        triggerInputs        = None
        initialPiloSignal    = None
        AllowedOptions       = ["C","P"]
        CompareDollarVolume  = True
        piloSignalDetection  = {}
        piloAlertTime        = None
        piloTriggerTime      = None
        piloTriggerTimeStr   = None
        piloConsecutiveTrigger = None
        LAST_BUY_TIME      = None
        piloTriggerTime = triggerTime + timedelta(seconds=piloTriggerOffset)
        piloTriggerTimeStr = piloTriggerTime.strftime("%Y-%m-%d %H:%M:%S")
        if piloTriggerOffset in [60]:
            piloTriggerTimeStr = piloTriggerTime.strftime("%Y-%m-%d %H:%M")
        if reqOptionType:                           #If reqOptionType is defined, then just set it to whatever the local Symbol is
            AllowedOptions = [reqOptionType]
        #if triggerTime is None:
        #    #triggerTime = (datetime.now() - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:00')
        #    triggerTime = datetime.now() - timedelta(minutes=1)
        #marketOpen = triggerTime.replace(hour=6, minute=30, second=00, microsecond=00)
        openSeconds = (triggerTime - marketOpen).seconds
        openMinutes = int(openSeconds/60)+1
        
        #If multiple triggers occurred, choose the one with the most Dollar-volume based on the "close" price.
        #print("Trigger Time: %s"%triggerTime)
        for localSymbol,df in OptionDataDict.items():
            props = self.op.getTriggerProperties(localSymbol, openMinutes)
            earliestTriggerTime = datetime.strptime(props["EARLIEST_BUY_TIME"],"%H:%M:%S") - timedelta(seconds=piloTriggerOffset) #Trigger happens 1 minute before the BUY can happen
            if earliestTriggerTime:
                earliestTriggerTime = datetime.combine(triggerTime.date(),earliestTriggerTime.time())
            lastBuyTime = datetime.strptime(props["LAST_BUY_TIME"],"%H:%M:%S")
            if lastBuyTime:
                lastBuyTime = datetime.combine(triggerTime.date(),lastBuyTime.time())
            lastTriggerTime = lastBuyTime - timedelta(seconds=piloTriggerOffset)         #Trigger happens 1 minute before the BUY can happen
            if len(AllowedOptions) < 2 and "TURN_OFF_FORCE_NEXT_TRADE" in props:
                AllowedOptions       = ["C","P"]
            ForcePiloBuyTime = props.get("FORCE_PILO_BUY")
            if ForcePiloBuyTime is not None:
                ForcePiloBuyTime = datetime.strptime(ForcePiloBuyTime,"%H:%M:%S")
                ForcePiloBuyTime = datetime.combine(triggerTime.date(), ForcePiloBuyTime.time())
            #Check if we already have an entry for the Put/Call piloDetection Dictionary. (So we don't re-check the same thing over and over)
            if localSymbol[self.PUTCALLIDX] not in piloSignalDetection.keys() and "PILO_ENTRY" in props:  #We run this loop once for Calls and once for puts
                for timePeriod,alertsDF in piloAlertsDict.items():
                    if timePeriod == props["PILO_ENTRY"][buyPosition] and not alertsDF.empty:
                        piloConsecutiveTrigger = props.get("PILO_CONSECUTIVE_TRIGGER",None)
                        buys = 0
                        sells = 0
                        for index,alertSignal in alertsDF.iterrows():
                            #Alerts apply to a previous time period; previous minute or previous 30 seconds
                            alertTime = alertSignal['date']
                            if alertTime <= piloTriggerTime:
                                alertTimeStr = alertTime.strftime("%Y-%m-%d %H:%M:%S")
                                #print(alertTimeStr,piloTriggerTimeStr)
                                if piloTriggerOffset in [60]:
                                    alertTimeStr = alertTime.strftime("%Y-%m-%d %H:%M")
                                if ForcePiloBuyTime is not None:
                                    if alertTime <= (ForcePiloBuyTime + timedelta(seconds=10)):
                                        initialPiloSignal = alertSignal['action']
                                        #print("setting initialPiloSignal: %s,%s,%s"%(alertTime,initialPiloSignal,triggerTime))
                                    elif initialPiloSignal == None:
                                        #We do not trade this condition if we didn't get a piloSignal before the FORCE_PILO_BUY time.
                                        return(buyContractKey, HighVoteVolume, HighVoteDollarVolume, HighVotetriggerPrice, triggerTime, BuySignalprops, piloAlertTime, LAST_BUY_TIME)
                                #print(alertTimeStr,piloTriggerTimeStr)
                                if alertTimeStr == piloTriggerTimeStr:
                                    #print("PiloAlert Detected at %s"%piloTriggerTimeStr)
                                    piloAlertTime = alertSignal['date']
                                    if alertSignal['action'] == "BUY" and (piloConsecutiveTrigger is None or sells >= piloConsecutiveTrigger):
                                        piloSignalDetection[localSymbol[self.PUTCALLIDX]] = "C"
                                        #print("Looking for %s"%piloSignalDetection[localSymbol[self.PUTCALLIDX]])
                                    elif alertSignal['action'] == "SELL" and (piloConsecutiveTrigger is None or buys >= piloConsecutiveTrigger):
                                        piloSignalDetection[localSymbol[self.PUTCALLIDX]] = "P"
                                        #print("Looking for %s"%piloSignalDetection[localSymbol[self.PUTCALLIDX]])
                                if piloConsecutiveTrigger:
                                    if alertSignal['action'] == "BUY":
                                        buys = buys + 1
                                        sells = 0
                                    elif alertSignal['action'] == "SELL":
                                        sells = sells + 1
                                        buys = 0
                        if localSymbol[self.PUTCALLIDX] not in piloSignalDetection.keys():  #We didn't find a piloAlert Match, so set to None.
                            piloSignalDetection[localSymbol[self.PUTCALLIDX]] = None
                    if timePeriod == props["PILO_ENTRY"][buyPosition] and alertsDF.empty:   #We had no data for this time period so return empty
                        return(buyContractKey, HighVoteVolume, HighVoteDollarVolume, HighVotetriggerPrice, triggerTime, BuySignalprops, piloAlertTime, LAST_BUY_TIME)
                if initialPiloSignal is not None and ForcePiloBuyTime == (triggerTime + timedelta(minutes=1)):
                    piloAlertTime = triggerTime + timedelta(minutes=1)
                    if initialPiloSignal == "BUY":
                        piloSignalDetection[localSymbol[self.PUTCALLIDX]] = "C"
                        #print("Looking for %s"%piloSignalDetection[localSymbol[self.PUTCALLIDX]])
                    else:
                        piloSignalDetection[localSymbol[self.PUTCALLIDX]] = "P"
                        #print("Looking for %s"%piloSignalDetection[localSymbol[self.PUTCALLIDX]])
            ###############################################
            if localSymbol not in self.FIRST_MINUTE_DATA_OFFSET.keys() and not df.empty:        #This code allows us to use iloc which is much faster for pulling rows from df
                firstRowOffset = int((df.iloc[[0]].date.item() - marketOpen).seconds/optionDataOffset)
                self.FIRST_MINUTE_DATA_OFFSET[localSymbol] = firstRowOffset
            ###############################################
            try:
                dfIndex = -1
                #dfIndex = openMinutes -1 - self.FIRST_MINUTE_DATA_OFFSET[localSymbol]
                dfIndex = int(openSeconds/optionDataOffset - self.FIRST_MINUTE_DATA_OFFSET[localSymbol])
                triggerInputs = df.iloc[dfIndex]
                if triggerInputs['date'] != triggerTime and optionDataOffset == piloTriggerOffset:
                    #print("Returning empty dataframe condition 1")
                    triggerInputs = pd.DataFrame()   #This returns an empty dataframe
                elif triggerInputs['date'].strftime("%H:%M") != triggerTime.strftime("%H:%M") and optionDataOffset != piloTriggerOffset:
                    #print("Returning empty dataframe condition 2")
                    triggerInputs = pd.DataFrame()   #This returns an empty dataframe
                    #triggerInputs = df.loc[df['date'] == triggerTime]   #This is used to return null if the trigger is not in the dataset
                #print(triggerInputs)
            except Exception as detail:
                print("Error:%s; Indexer: %d; Returning empty dataframe"%(detail,dfIndex))
                try:
                    firstRowOffset = int((df.iloc[[0]].date.item() - marketOpen).seconds/optionDataOffset)
                    self.FIRST_MINUTE_DATA_OFFSET[localSymbol] = firstRowOffset
                except Exception as detail:
                    print("At least we tried")
                    print(detail)
                triggerInputs = pd.DataFrame()   #This returns an empty dataframe
                #triggerInputs = df.loc[df['date'] == triggerTime]   #returns empty dataframe when this is used
                dfIndex = None
            if overRideProps:
                for key, value in overRideProps.items():
                    props[key] = value
            if localSymbol[self.PUTCALLIDX] in AllowedOptions and self.TriggerCriteriaSatisfied(df, props, localSymbol, openMinutes, buyPosition, triggerTime, triggerInputs, mrkt_gap, dfIndex, piloSignalDetection.get(localSymbol[self.PUTCALLIDX]), earliestTriggerTime, lastTriggerTime):
                #print(triggerTime)
                TriggerVolume  = triggerInputs['volume'].item()
                TriggerClose   = triggerInputs['close'].item()
                TriggerAvg     = triggerInputs['average'].item()
                #if buyContractKey is None or (buyContractKey and (TriggerVolume*TriggerAvg*100 > HighVoteDollarVolume and localSymbol[self.PUTCALLIDX]==self.PREFERRED_OPTION)):
                if self.PREFERRED_OPTION and buyContractKey and localSymbol[self.PUTCALLIDX]!=buyContractKey[self.PUTCALLIDX] and localSymbol[self.PUTCALLIDX]==self.PREFERRED_OPTION:
                    #Don't compare volume if the current buyContractKey is not the preferred option and we have received a preferred trigger.
                    CompareDollarVolume = False
                if buyContractKey is None or not CompareDollarVolume or TriggerVolume*TriggerAvg*100 > HighVoteDollarVolume:
                    buyContractKey = localSymbol
                    HighVoteVolume = TriggerVolume
                    HighVoteDollarVolume = TriggerVolume*TriggerAvg*100
                    HighVotetriggerPrice = TriggerClose
                    HighVoteLowPrice = triggerInputs['low'].item()
                    BuySignalprops = props
                    LAST_BUY_TIME = lastBuyTime
                    #print("Triggered: %s  DollarVol: %.2f  Volume:%d TriggerPrice: %.2f at Time %s"%(buyContractKey,HighVoteDollarVolume,HighVoteVolume,HighVotetriggerPrice,triggerTime))
        if buyContractKey:
            self.PREFERRED_OPTION = None
            if "PRICE_ATTRIBUTES" in BuySignalprops:
                lookupKey = str(int(TriggerClose*10)*10) 
                if lookupKey in BuySignalprops["PRICE_ATTRIBUTES"]:
                    if "TARGET" in BuySignalprops["PRICE_ATTRIBUTES"][lookupKey]:
                        BuySignalprops["TARGET"] = BuySignalprops["PRICE_ATTRIBUTES"][lookupKey]["TARGET"]
                        BuySignalprops.pop("STOPADJUSTTRIGGER", None)  #Remove these modifiers that could change the TARGET
                        BuySignalprops.pop("GAPADJTRGTTRIGGER", None)  #Remove these modifiers that could change the TARGET
                    if "STOP" in BuySignalprops["PRICE_ATTRIBUTES"][lookupKey]:
                        BuySignalprops["STOP"] = BuySignalprops["PRICE_ATTRIBUTES"][lookupKey]["STOP"]
                    if "LARGE_GAP_TRIGGER" in BuySignalprops and abs(mrkt_gap) > float(BuySignalprops["LARGE_GAP_TRIGGER"]) and "LARGE_GAP_TARGET" in BuySignalprops["PRICE_ATTRIBUTES"][lookupKey]:
                        BuySignalprops["TARGET"] = BuySignalprops["PRICE_ATTRIBUTES"][lookupKey]["LARGE_GAP_TARGET"]
                        BuySignalprops.pop("STOPADJUSTTRIGGER", None)  #Remove these modifiers that could change the TARGET
                        BuySignalprops.pop("GAPADJTRGTTRIGGER", None)  #Remove these modifiers that could change the TARGET
            if "MAXTRIGGERPERCENTOFFMINUTELOW" in BuySignalprops:
                distanceFromLow = (HighVotetriggerPrice - HighVoteLowPrice)*100/HighVoteLowPrice
                print("Distance Off minute Low: %.2f%%"%(distanceFromLow))
                if distanceFromLow > BuySignalprops["MAXTRIGGERPERCENTOFFMINUTELOW"]*100:
                    HighVotetriggerPrice = HighVoteLowPrice*(1+BuySignalprops["MAXTRIGGERPERCENTOFFMINUTELOW"])
                    print("Trigger adjusted to %.2f%% off of low"%float(BuySignalprops["MAXTRIGGERPERCENTOFFMINUTELOW"]*100))
        return(buyContractKey, HighVoteVolume, HighVoteDollarVolume, HighVotetriggerPrice, triggerTime, BuySignalprops, piloAlertTime, LAST_BUY_TIME)

    def TriggerCriteriaSatisfied(self, df, props, localSymbol, openMinutes, buyPosition, triggerTime, triggerTimeInputs, mrkt_gap, dfIndex, piloSignalDetected, earliestTriggerTime, lastTriggerTime):
        MINDISTANCEFROMHIGH = props["MINDISTANCEFROMHIGH"]["BUY"+str(buyPosition)]
        VOLUMETRIGGER = props["TRIGGER_VOLUME"]
        MAX_TRADE_AVERAGE = None
        MIN_TRADE_AVERAGE = None
        DOLLARVOLUME = props["TRIGGER_DOLVOL"]["BUY"+str(buyPosition)]
        OPTIONMULTIPLIER = 100
        SAMEDAYEXPIRATION = False
        expiration = localSymbol[6:12]
        todayDate = triggerTime.strftime('%y%m%d')
        tradeable = True
        force_tradeable = False
        if "SAMEDAY_VOLUME" in props:
            if expiration == todayDate:  #USE SAMEDAY_VOLUME if option expires today
                VOLUMETRIGGER = props["SAMEDAY_VOLUME"]
                SAMEDAYEXPIRATION = True
            else:  #This rule forces program to only trade on SAMEDAYEXPIRATION days.
                return False
        morningsentiment = datetime.strptime("06:30:00","%H:%M:%S")
        try:
            Volume     = triggerTimeInputs['volume'].item()
            TriggerAvg = triggerTimeInputs['average'].item()
            Close      = triggerTimeInputs['close'].item()
            Low        = triggerTimeInputs['low'].item()
            Trades     = triggerTimeInputs['barCount'].item()
            dolVol     = Volume*TriggerAvg*OPTIONMULTIPLIER
            self.track_Extremes(df, triggerTime, localSymbol, triggerTimeInputs, "high")
            self.track_Extremes(df, triggerTime, localSymbol, triggerTimeInputs, "low")
            self.track_Extremes(df, triggerTime, localSymbol, triggerTimeInputs, "volume")
            if "PRICE_ATTRIBUTES" in props:
                lookupKey = str(int(Close*10)*10) 
                if lookupKey in props["PRICE_ATTRIBUTES"]:
                    try:
                        VOLUMETRIGGER = props["PRICE_ATTRIBUTES"][lookupKey]["TRIGGER_VOLUME"]
                    except: pass
                    try:
                        props["TRIGGER_TRADES"]    = props["PRICE_ATTRIBUTES"][lookupKey]["TRIGGER_TRADES"]
                    except: pass
                    try:
                        props["MAX_VOLUME"] = props["PRICE_ATTRIBUTES"][lookupKey]["MAX_VOLUME"]
                    except: pass
                    try:
                        if self.MAX_DAILY_PRICE[localSymbol] < props["PRICE_ATTRIBUTES"][lookupKey]["PRETRIGGERHIGHMINIMUM"]:
                            return False
                    except: pass
                    try:
                        MAX_TRADE_AVERAGE = float(props["PRICE_ATTRIBUTES"][lookupKey]["MAX_TRADE_AVERAGE"])
                    except: pass
                    try:
                        MIN_TRADE_AVERAGE = float(props["PRICE_ATTRIBUTES"][lookupKey]["MIN_TRADE_AVERAGE"])
                    except: pass
                        
            if localSymbol not in self.DAILY_OPEN_PRICE.keys():            #Save the Day's opening price using index 0 of the df; may not be 6:30, but first trade#
                self.DAILY_OPEN_PRICE[localSymbol] = df.iloc[[0]]['open'].item()
            #######Here we are saving all the big spikes of the day to monitor for possible buys and sells of the same volume size.###
            if "FILTER_SELLS" in props and Volume>=VOLUMETRIGGER:
                if localSymbol in self.VOLUME_TRACKER.keys():
                    self.VOLUME_TRACKER[localSymbol].append(triggerTimeInputs)
                else:
                    self.VOLUME_TRACKER[localSymbol] = [triggerTimeInputs]
                for trigger in self.VOLUME_TRACKER[localSymbol]:
                    if Volume!=trigger['volume'].item() and Volume <= (trigger['volume'].item()+int(props["FILTER_SELLS"])) and Volume >= (trigger['volume'].item()-int(props["FILTER_SELLS"])) and TriggerAvg >= trigger['average'].item():
                        #print("%s Possible Sell at %s"%(localSymbol,triggerTime))
                        return False
            if "PREFERRED_OPTION" in props and props["PREFERRED_OPTION"] and triggerTime.time() == morningsentiment.time() and Volume < 10:
                self.PREFERRED_OPTION = self.INVERSE_OPTION_DICT[localSymbol[self.PUTCALLIDX]]
            if (triggerTime >= earliestTriggerTime and triggerTime <= lastTriggerTime): #This removes the date portion; compares just time
                if "ONLY_BIGGEST_VOLUME" in props and props["ONLY_BIGGEST_VOLUME"]:
                    if Volume < self.MAX_DAILY_VOLUME[localSymbol]:
                        return False
                if "MAXPRICE" in props and Close > props["MAXPRICE"]:
                    return False
                if "MINPRICE" in props and Close < props["MINPRICE"]:
                    return False
                if "MAX_OPEN_PRICE" in props and self.DAILY_OPEN_PRICE[localSymbol] > props["MAX_OPEN_PRICE"]:
                    return False
                if "MAX_VOLUME" in props and Volume > props["MAX_VOLUME"]:
                    return False
                if "TRIGGER_TRADES" in props and Trades < props["TRIGGER_TRADES"]:
                    return False
                if "TWO_MIN_TRADES" in props and dfIndex >= 2:
                    if df.iloc[[dfIndex-1]]['barCount'].item() < props["TWO_MIN_TRADES"] or Trades < props["TWO_MIN_TRADES"]:
                        return False
                if "TWO_MIN_DOLVOL" in props and dfIndex >= 2:
                    previousDolVol = df.iloc[[dfIndex-1]]['volume'].item()*df.iloc[[dfIndex-1]]['average'].item()*OPTIONMULTIPLIER
                    if (previousDolVol < props["TWO_MIN_DOLVOL"] or dolVol < props["TWO_MIN_DOLVOL"]):
                        return False
                if "FIVE_MIN_DOLVOL" in props and dfIndex >= 5:
                    for previousMinute in [-5,-4,-3,-2,-1]:
                        if df.iloc[[dfIndex+previousMinute]]['volume'].item()*df.iloc[[dfIndex+previousMinute]]['average'].item()*OPTIONMULTIPLIER < props["FIVE_MIN_DOLVOL"]:
                            return False
                if "VOLUME_DEAD_ZONE" in props and Volume > props["VOLUME_DEAD_ZONE"][0] and Volume < props["VOLUME_DEAD_ZONE"][1]:
                    return False
                if "MAX_MKT_GAP" in props and mrkt_gap > props["MAX_MKT_GAP"]:
                    return False
                if "MIN_MKT_GAP" in props and mrkt_gap < props["MIN_MKT_GAP"]:
                    return False
                if "DAILY_RANGE_FILTER" in props:
                    currentRange =  (TriggerAvg - self.MIN_DAILY_PRICE[localSymbol])/(self.MAX_DAILY_PRICE[localSymbol] - self.MIN_DAILY_PRICE[localSymbol])
                    if props["DAILY_RANGE_FILTER"]["MODIFIER"].upper() == "ABOVE" and currentRange < props["DAILY_RANGE_FILTER"]["RANGE"]:
                        return False
                    elif props["DAILY_RANGE_FILTER"]["MODIFIER"].upper() == "BELOW" and currentRange > props["DAILY_RANGE_FILTER"]["RANGE"]:
                        return False
                #PILO_SIGNAL_RETURNS HERE; ONLY RULES ABOVE ARE EVALUATED FOR PILO ALERTS!!!
                if piloSignalDetected:
                    if piloSignalDetected == localSymbol[self.PUTCALLIDX]:
                        return True
                    else:
                        return False
                if "USE_ONLY_PILO_ENTRY" in props and props["USE_ONLY_PILO_ENTRY"]:  #We didn't pick up a pilo signal so return False
                    return False
                #PILO_SIGNAL_RETURNS HERE; Only rules above are evaluated for pilo alerts
                if "FIVEMINVOLUMEOVERRIDE" in props and openMinutes>=8:
                    THREEMINUTEVOL = props["FIVEMINVOLUMEOVERRIDE"]
                    FiveMinVol = df[openMinutes-5:openMinutes]['volume'].sum()
                    if FiveMinVol >= props["FIVEMINVOLUMEOVERRIDE"]:
                        print("Five Minute Volume Triggered @ %s with %d for %s"%(triggerTime, FiveMinVol, localSymbol[self.PUTCALLIDX]))
                        if "FIVEMINVOLUMETRADE" in props and props["FIVEMINVOLUMETRADE"]:   #If this is not true, we are just printing the data for information purposes only; may do something in the future
                            force_tradeable = tradeable and True
                if "TWO_SIGNAL_VOLUME" in props and openMinutes >=2:
                    two_signal_volume = props["TWO_SIGNAL_VOLUME"]
                    if Volume >= two_signal_volume:
                        if triggerTime in self.TWO_SIGNAL_VOLUME:
                            if localSymbol not in self.TWO_SIGNAL_VOLUME[triggerTime]:
                                self.TWO_SIGNAL_VOLUME[triggerTime].append(localSymbol)
                        else:
                            self.TWO_SIGNAL_VOLUME[triggerTime] = [localSymbol]
                        if len(self.TWO_SIGNAL_VOLUME[triggerTime]) > 1:
                            optionType = localSymbol[self.PUTCALLIDX]
                            for symbol in self.TWO_SIGNAL_VOLUME[triggerTime]:
                                if symbol[self.PUTCALLIDX] != optionType:  #There is a conflicting trigger for the same minute but opposite option type
                                    force_tradeable = False
                            force_tradeable = True                      #Two or more volume triggers of the same option type identified removes additional trigger limitations
                if not force_tradeable and (Volume < VOLUMETRIGGER or dolVol < DOLLARVOLUME or TriggerAvg > (self.MAX_DAILY_PRICE[localSymbol]*(1.0-MINDISTANCEFROMHIGH))):
                    return False
                if "SPIKEONLOWOFDAY" in props:
                    if Low != self.MIN_DAILY_PRICE[localSymbol]:
                        return False
                if "MINLOWPERCENTFROMDAYLOW" in props:
                    distanceFromDayLow = (Low - self.MIN_DAILY_PRICE[localSymbol])/self.MIN_DAILY_PRICE[localSymbol]
                    if tradeable and distanceFromDayLow > props["MINLOWPERCENTFROMDAYLOW"]:
                        return False
                if "VOLUME_MULTIPLE_OF_AVERAGE" in props and dfIndex > 5:
                    sum = 0
                    for previousMinute in [-1,-2,-3,-4,-5]:
                        sum = sum + df.iloc[[dfIndex+previousMinute]]['volume'].item()
                    if Volume < sum/5*float(props["VOLUME_MULTIPLE_OF_AVERAGE"]):
                        return False
                if "TRADE_MULTIPLE_OF_AVERAGE" in props and dfIndex > 5:
                    sum = 0
                    for previousMinute in [-1,-2,-3,-4,-5]:
                        sum = sum + df.iloc[[dfIndex+previousMinute]]['barCount'].item()
                    if Trades < sum/5*float(props["TRADE_MULTIPLE_OF_AVERAGE"]):
                        return False
                if MAX_TRADE_AVERAGE:
                    sum = 0
                    for previousMinute in [-1,-2,-3,-4,-5]:
                        sum = sum + df.iloc[[dfIndex+previousMinute]]['barCount'].item()
                    if sum/5 > MAX_TRADE_AVERAGE:
                        return False
                if MIN_TRADE_AVERAGE:
                    sum = 0
                    for previousMinute in [-1,-2,-3,-4,-5]:
                        sum = sum + df.iloc[[dfIndex+previousMinute]]['barCount'].item()
                    if sum/5 < MIN_TRADE_AVERAGE:
                        return False
                return((tradeable and dolVol>=DOLLARVOLUME and Volume>=VOLUMETRIGGER and TriggerAvg<=(self.MAX_DAILY_PRICE[localSymbol]*(1.0-MINDISTANCEFROMHIGH))) or force_tradeable)
            else:
                return False
        except Exception as detail:
            #print("ERROR: Missing triggerTime from dataframe %s"%triggerTime)
            if triggerTime.time() == morningsentiment.time():
                self.PREFERRED_OPTION = self.INVERSE_OPTION_DICT[localSymbol[self.PUTCALLIDX]]
            return False

    '''Here we are saving constantly tracking the daily extreme values. It is quicker to track this manually than to search the whole data set every minute for the max and min'''
    def track_Extremes(self, df, triggerTime, localSymbol, triggerTimeInputs, tracking_variable):
        MAX_DICT = {"high": self.MAX_DAILY_PRICE,
                    "volume": self.MAX_DAILY_VOLUME
                   }
        MIN_DICT = {"low": self.MIN_DAILY_PRICE
                   }
        if tracking_variable in MAX_DICT.keys() and localSymbol in MAX_DICT[tracking_variable].keys():
            if not triggerTimeInputs.empty and float(triggerTimeInputs[tracking_variable].item()) > float(MAX_DICT[tracking_variable][localSymbol]):
                MAX_DICT[tracking_variable][localSymbol] = triggerTimeInputs[tracking_variable].item()
        elif tracking_variable in MAX_DICT.keys():
            dailyHighs = df[df.date <= triggerTime][tracking_variable]
            if not dailyHighs.empty:
                MAX_DICT[tracking_variable][localSymbol] = dailyHighs.max()
        elif tracking_variable in MIN_DICT.keys() and localSymbol in MIN_DICT[tracking_variable].keys():
            if not triggerTimeInputs.empty and float(triggerTimeInputs[tracking_variable].item()) < float(MIN_DICT[tracking_variable][localSymbol]):
                MIN_DICT[tracking_variable][localSymbol] = triggerTimeInputs[tracking_variable].item()
        elif tracking_variable in MIN_DICT.keys():
            dailyLows = df[df.date <= triggerTime][tracking_variable]
            if not dailyLows.empty:
                MIN_DICT[tracking_variable][localSymbol] = dailyLows.min()

    def getLowestDrawPointBetweenPoints(self, df, point1, point2):
        newDF = df[point1:point2+1]
        return newDF['low'].min()


    def getDailyReturnPercent(self, df, purchasePrice, props, MaxDrawperc, maxGainPercent, closeDrawPerc, closeGainPerc, closeTime, maxGainIndex,  stopped, stopExitPrice, stopExitTime, earlyExitSignalTime, targetPrice, targetHitTime, piloExitPrice, piloExitTime, sellAlertSeconds):
        dailyReturn = None
        sellTime = None
        piloExited = False
        if sellAlertSeconds and sellAlertSeconds < 25:  #Give the piloalert credit for the sell if it is in the first half of the minute
            piloExited = True
        if targetHitTime:
            dailyReturn = (targetPrice - purchasePrice)/purchasePrice*100
            exitPrice = targetPrice
            sellTime = targetHitTime
        if piloExitTime:
            if (sellTime and piloExitTime < sellTime) or sellTime is None:
                dailyReturn = (piloExitPrice - purchasePrice)/purchasePrice*100
                sellTime = piloExitTime
                exitPrice = piloExitPrice
        if earlyExitSignalTime:
            print("Found earlyExitSignalTime")
            exitPrice = purchasePrice*(1+props["EARLYEXITTARGET"])
            remainingHighs = df[df.date >= earlyExitSignalTime]
            earlyExitSellTime = remainingHighs[remainingHighs.high > exitPrice].iloc[0].date  #Require a high above our target to ensure a sell.
            if (sellTime and earlyExitSellTime < sellTime) or sellTime is None:
                print("Submitted early Sell")
                sellTime = earlyExitSellTime
                dailyReturn = props["EARLYEXITTARGET"]*100
        if stopped and (sellTime==None or (piloExitTime and piloExitTime == stopExitTime and not piloExited) or sellTime > stopExitTime):
            dailyReturn = (stopExitPrice - purchasePrice)*100/purchasePrice
            exitPrice = stopExitPrice
            sellTime = stopExitTime
            #print("                                                 Stopped out: %0.2f%% at %s"%((stopExitPrice - purchasePrice)*100/purchasePrice,stopExitTime.strftime('%H:%M')))
        if dailyReturn == None:
            dailyReturn = closeGainPerc
            exitPrice = purchasePrice*(1+closeGainPerc/100)
            sellTime = closeTime
        if piloExitTime and sellTime == piloExitTime:
            props["PILO_EXITED"] = True
        return dailyReturn, sellTime

    def getLimitPrices(self, triggerPrice, TriggerProperties, overNightGap=None):
        D = decimal.Decimal
        cent = D('0.01')
        purchasePrice = triggerPrice*(1+TriggerProperties["TRIGGER_DISCOUNT"])  #Factor in if we are looking for a discount
        purchasePrice = float(D('%.4f'%purchasePrice).quantize(cent,rounding=decimal.ROUND_DOWN))  #round down to nearest cent
        targetPrice = purchasePrice*(1+TriggerProperties["TARGET"])
        targetPrice = float(D('%.4f'%targetPrice).quantize(cent,rounding=decimal.ROUND_UP))  #round up to nearest cent
        stopPrice   = purchasePrice*(1+TriggerProperties["STOP"])
        stopPrice   = float(D('%.4f'%stopPrice).quantize(cent,rounding=decimal.ROUND_UP))  #round up to nearest cent
        return purchasePrice, targetPrice, stopPrice

    def getMaxGainMaxLossFromDF(self, localSymbol, MinuteDataDict, props, triggerTime, buyPosition, marketDailyOpenTime, marketClose, triggerPrice, overNightGap, piloAlertsDict, piloAlertTime, nextTradeDay):
        df = MinuteDataDict[localSymbol]
        optionType = localSymbol[self.PUTCALLIDX]
        purchasePrice = targetPrice = None
        purchaseIndex = None
        LOC = 0
        VAL = 1
        D = decimal.Decimal
        cent = D('0.01')
        maxGain = {}
        maxDraw = {}
        bought = False
        targetHit = False
        targetHitTime = None
        sameDayExpirationDataDetected = False
        lastDate = None
        stopped = False
        piloExitTime = None
        piloExitPrice = None
        stopExitTime = None
        stopExitPrice = None
        stopAdjusted = False
        trailingStop = False
        earlyExitSignalTime = None
        stopTarget = None
        triggerIndex = None
        triggerClose = None
        FiveMinSumDict = {"Trades":0,"High":0,"Low":0,"Volume":0,"DolVol":0}
        PostTriggerSumDict = {"Trades":0,"High":0,"Low":0,"Volume":0,"DolVol":0}
        TriggerRow = None
        PostTriggerVolumes = []
        OpenClose = []
        nextTradeDate = datetime.strptime(nextTradeDay,"%Y%m%d")
        nextDayOpenTime = datetime.combine(nextTradeDate.date(),datetime.strptime("06:30:00","%H:%M:%S").time())
        CloseDrawPerc = CloseGainPerc = CloseTime = dailyReturn = sellAlertSeconds = None
        last_buy_time = datetime.strptime(props["LAST_BUY_TIME"],"%H:%M:%S")
        last_buy_time = datetime.combine(triggerTime.date(),last_buy_time.time())
        if marketDailyOpenTime > len(df.index):
            print("!!!!MISSING DATA FROM HISTORY!!!!")
        ClosePositionTime = marketClose - timedelta(minutes=props["EODSELLMINUTESFROMCLOSE"])
        if self.OVERNIGHT:
            ClosePositionTime = datetime.combine(nextTradeDate.date(), marketClose.time()) - timedelta(minutes=props["EODSELLMINUTESFROMCLOSE"])
            ForcePiloBuyTime = props.get("FORCE_PILO_BUY")
            if ForcePiloBuyTime is not None:
                ForcePiloBuyTime = datetime.strptime(ForcePiloBuyTime,"%H:%M:%S")
                ClosePositionTime = datetime.combine(nextTradeDate.date(), ForcePiloBuyTime.time())

        for index,row in df.iterrows():
            if index == 0:
                OpenClose.append(row['open'])
            if row.date == marketClose:
                OpenClose.append(row['close'])
            if self.OVERNIGHT and lastDate is None:   #Initialize our lastDate variable
                lastDate = row.date.date()
            minutesTillTrigger = -int((triggerTime - row.date).seconds/60)
            if self.VERBOSE and not bought and minutesTillTrigger in [-5,-4,-3,-2,-1,0]:
                DolVol = row['average']*row['volume']*100
                DolVolStr = "${:,.2f}".format(DolVol)
                if minutesTillTrigger == 0:
                    TriggerRow = row
                    print("Minute %d - Volume: %d/%s,  High: %.2f, Low: %.2f, Trades: %d, AvgTrades: %d"%(minutesTillTrigger,row['volume'],DolVolStr,row['high'],row['low'],row['barCount'],FiveMinSumDict['Trades']/5))
                else:
                    FiveMinSumDict = self.AddValuesToDict(FiveMinSumDict, row['barCount'], row['high'], row['low'], row['volume'], DolVol)
                    print("Minute %d - Volume: %d/%s,  High: %.2f, Low: %.2f, Trades: %d"%(minutesTillTrigger,row['volume'],DolVolStr,row['high'],row['low'],row['barCount']))
            if not bought and row.date == triggerTime:
                triggerIndex = index
                triggerVolume = row['volume']
                triggerAvgPrice = row['average']
                triggerDolVolume = triggerVolume*triggerAvgPrice
                triggerClose  = row['close']
                purchasePrice, targetPrice, stopPrice = self.getLimitPrices(triggerPrice, triggerVolume, props, buyPosition, overNightGap)
            elif purchasePrice and row.date > triggerTime:
                #if "FILTER_SELLS" in props and row['volume']>=props["TRIGGER_VOLUME"]:  #FOLLOWUP: With this removed, we are not tracking while in a trade
                #    self.VOLUME_TRACKER[localSymbol].append(row)
                if self.VERBOSE and (index - triggerIndex) in [1,2,3,4,5]:
                    PostTriggerVolumes.append(row['volume'])
                    DolVol = row['average']*row['volume']*100
                    DolVolStr = "${:,.2f}".format(DolVol)
                    PostTriggerSumDict = self.AddValuesToDict(PostTriggerSumDict, row['barCount'], row['high'], row['low'], row['volume'], DolVol)
                    print("Minute %d - Volume: %d/%s,  High: %.2f, Low: %.2f, Trades: %d"%((index - triggerIndex),row['volume'],DolVolStr,row['high'],row['low'],row['barCount']))
                #if not bought and row.date.time() <= last_buy_time.time() and (row.date - triggerTime).seconds/60 < props["EXPIRATION"]: #Check for valid buy window
                if not bought and row.date < marketClose and (row.date - triggerTime).seconds/60 < props["EXPIRATION"]: #Check for valid buy window
                    if ("ORDER_TYPE" not in props or props["ORDER_TYPE"][buyPosition]=="LMT") and row['low'] < purchasePrice:
                        bought = True
                    elif "ORDER_TYPE" in props and props["ORDER_TYPE"][buyPosition]=="MKT":
                        bought = True
                        purchasePrice = row['open']
                        if (interval is None or interval in ["1min"]) and piloAlertTime is not None:
                            BuyAlertSeconds = int(piloAlertTime.strftime("%S")) + 5
                            if BuyAlertSeconds > 30:
                                purchasePrice = (row['high'] + row['low'])/2  #Assume buy happened somewhere in the middle
                                purchasePrice = float(D('%.4f'%purchasePrice).quantize(cent,rounding=decimal.ROUND_UP))  #round up to nearest cent
                    #Look for exit time once we have the position#
                    if bought:
                        purchaseIndex = index
                        purchaseTime = row.date
                        maxGain = {"Value":purchasePrice, "Index":index}
                        maxDraw = {"Value":purchasePrice, "Index":index}
                        DailyGainAtBuyTime = (purchasePrice - self.DAILY_OPEN_PRICE[localSymbol])/self.DAILY_OPEN_PRICE[localSymbol]
                        DistanceOffLowOfDay = (purchasePrice - self.MIN_DAILY_PRICE[localSymbol])/self.MIN_DAILY_PRICE[localSymbol]
                        print("bought at %s after gain from open of %.2f and off a low of %.2f"%(row.date.strftime('%Y-%m-%d %H:%M:%S'),DailyGainAtBuyTime,DistanceOffLowOfDay))
                        if "PILO_EXIT" in props and props["PILO_EXIT"][buyPosition] in piloAlertsDict:
                            alertsDF = piloAlertsDict[props["PILO_EXIT"][buyPosition]]
                            if not alertsDF.empty:
                                alertsDF = alertsDF.loc[alertsDF['date'] >= row.date].reset_index()  #Filter to only alerts after our buy
                                if "LOCK_TRADE" in props and props["LOCK_TRADE"][buyPosition]:       #Filter to only alerts the next day
                                    print("Position Locked upon acquisition")
                                    alertsDF = alertsDF.loc[alertsDF['date'] > (marketClose + timedelta(minutes=1))]  #Pad with 1min buffer)
                                elif "LOCK_TRADE" in props:   #Lock trades after the buy window has closed
                                    alertsDF = alertsDF.loc[alertsDF['date'] <= last_buy_time]
                                    print("Position locked after last_buy_time")
                                exitSignal = "SELL"
                                if optionType == "P":
                                    exitSignal = "BUY"
                                for index,alertSignal in alertsDF.iterrows():
                                    if alertSignal['action'] == exitSignal:
                                        if alertSignal['date'] < ClosePositionTime and not (alertSignal['date'] > marketClose and alertSignal['date'] < nextDayOpenTime):  #Check that it was received during market hours
                                            piloExitTime = alertSignal['date']
                                            break
                if bought and row.date < ClosePositionTime+timedelta(minutes=1):
                    if row['high'] > maxGain['Value']:
                        maxGain['Value'] = row['high']
                        maxGain['Index'] = index
                    if not stopped and "STOPADJUSTTRIGGER" in props.keys() and not stopAdjusted and "SHOOTFORTHEMOON" not in props.keys() and (maxGain['Value'] - purchasePrice)/purchasePrice >= props["STOPADJUSTTRIGGER"]:
                        stopAdjusted = True
                        props["STOP"] = props["STOPADJUSTTARGET"]
                    #if "TRAILINGSTOPTRIGGER" in props.keys() and not trailingStop and "SHOOTFORTHEMOON" not in props.keys() and (maxGain['Value'] - purchasePrice)/purchasePrice >= props["TRAILINGSTOPTRIGGER"]:
                    #    trailingStop = True
                    #if not stopped and trailingStop:
                    #   stopExitPrice = maxGain['Value']*(1-props["TRAILINGSTOP"])
                    #   if df.iloc[[index]]['low'].item() < stopExitPrice:
                    #       props["STOP"] = 1 + (stopExitPrice - purchasePrice)/purchasePrice
                    #       stopped = True
                    if piloExitTime and not piloExitPrice:
                        if interval and interval in ["15s","30s"] and row['date'].strftime("%Y-%m-%d %H:%M:%S") >= piloExitTime.strftime("%Y-%m-%d %H:%M:%S"):
                            piloExitPrice = row['open']
                            if row['date'].strftime("%Y-%m-%d %H:%M:%S")!= piloExitTime.strftime("%Y-%m-%d %H:%M:%S"):
                                print("WARNING!!! The exit time used for this exit price was later than expected!!")
                            print("Pilo Exit Price: %.2f"%piloExitPrice)
                        elif (interval is None or interval in ["1min"]) and row['date'].strftime("%Y-%m-%d %H:%M") >= piloExitTime.strftime("%Y-%m-%d %H:%M"):
                            piloExitPrice = row['open']
                            if row['date'].strftime("%Y-%m-%d %H:%M") == piloExitTime.strftime("%Y-%m-%d %H:%M"):
                                sellAlertSeconds = int(piloExitTime.strftime("%S")) + 5  #5 seconds to account for clock drift
                                if sellAlertSeconds >= 30:
                                    piloExitPrice = (row['high'] + row['low'])/2      #assume we exited somewhere in the middle of the minute's range
                                    piloExitPrice = float(D('%.4f'%piloExitPrice).quantize(cent,rounding=decimal.ROUND_DOWN))  #round down to nearest cent
                            else:
                                piloExitTime = row['date']  #Not sure if I should update this or not based on the historical data; For now, don't update.
                                print("WARNING!!! The exit time used for this exit price was > 1 minute after the alert!!")
                            print("Pilo Exit Price: %.2f"%piloExitPrice)
                    if not stopped:
                        if not targetHit and row['high'] >= targetPrice:
                            targetHit = True
                            targetHitTime = row.date
                            if self.OVERNIGHT and row.date.date() != lastDate and row['open'] >= targetPrice:  #This finds the first minute of the next day
                                props["TARGET"] = (row['open']-purchasePrice)/purchasePrice
                                targetPrice = row['open']
                                print("Sold at the open")
                        if "LOSS2PROFITADJUSTED" in props:
                            if not targetHit and row['low'] < maxDraw['Value']:
                                maxDraw['Value'] = row['low']
                                maxDraw['Index'] = index
                                print("new low: %.2f; DrawPercent: %.2f"%(maxDraw['Value'],(maxDraw['Value'] - purchasePrice)/purchasePrice))
                                targetPercent = props["TARGET"] + (maxDraw['Value'] - purchasePrice)/purchasePrice
                                targetPrice = (1+targetPercent)*purchasePrice
                                targetPrice = float(D('%.4f'%targetPrice).quantize(cent,rounding=decimal.ROUND_UP))  #round up to nearest cent
                                print("new targetPrice: %.2f"%targetPrice)
                        stopExitPrice = (1+props["STOP"])*purchasePrice
                        if stopTarget and row['high'] > stopTarget:
                            stopExitTime = row.date
                            stopped = True
                            stopExitPrice = stopTarget
                        elif row['low'] < stopExitPrice:
                            if "EMERGENCY_STOP" in props.keys() and stopTarget is None: #Set target for desired stop#
                                stopTarget = stopExitPrice
                                #Drop stop order to the emergency exit
                                props["STOP"] = props["STOP"] + props["EMERGENCY_STOP"]  #Drop the current stop limit lower to allow target to possibly fill.
                                stopExitPrice = (1+props["STOP"])*purchasePrice          #Re-adjust stopExitPrice to new lower limit.
                        if not stopped and row['low'] < stopExitPrice:
                            stopExitPrice = (1+props["STOP"])*purchasePrice*(1-self.STOP_SLIPPAGE)
                            stopped = True
                            stopExitTime = row.date
                            if self.OVERNIGHT and row.date.date() != lastDate:  #This finds the first minute of the next day
                                stopExitPrice = row['open']*(1-self.STOP_SLIPPAGE)
                                print("Stopped at the open at %.2f"%stopExitPrice)
            if bought and row.date < (ClosePositionTime+timedelta(minutes=1)):  #Not great because we are missing some EOD data in April
                if not sameDayExpirationDataDetected and self.OVERNIGHT and row.date.strftime("%Y%m%d")==props["EXPIRATIONDATE"]:
                    sameDayExpirationDataDetected = True
                ClosePrice = row['close']*(1-self.STOP_SLIPPAGE)
                CloseTime = row.date
                CloseGainPerc = float(ClosePrice - purchasePrice)*100/purchasePrice
                LowestPoint = self.getLowestDrawPointBetweenPoints(df, maxGain['Index'], index)
                if LowestPoint > purchasePrice:
                    CloseDrawPerc = 0.0
                else:
                    CloseDrawPerc = float(LowestPoint - purchasePrice)*100/purchasePrice
            lastDate = row.date.date()
        if bought:
            loss2Profit = self.getLowestDrawPointBetweenPoints(df, purchaseIndex, maxGain['Index'])
            maxGainPercent = float(maxGain['Value'] - purchasePrice)*100/purchasePrice
            MaxDrawperc = float(loss2Profit - purchasePrice)*100/purchasePrice
            try:
                dailyReturn, sellTime = self.getDailyReturnPercent(df[purchaseIndex:], purchasePrice, props, MaxDrawperc, maxGainPercent, CloseDrawPerc, CloseGainPerc, CloseTime, maxGain['Index']-purchaseIndex, stopped, stopExitPrice, stopExitTime, earlyExitSignalTime, targetPrice, targetHitTime, piloExitPrice, piloExitTime, sellAlertSeconds)
            except Exception as detail:
                print(detail)
                print(traceback.format_exc())
                print("Exception thrown trying to get daily return percent; missing data?")
                dailyReturn = 0.0
                sellTime = datetime.now()
            if self.OVERNIGHT and not sameDayExpirationDataDetected and underlying in ["SPY","QQQ"]:
                print("Missing SAMEDAYEXPIRATION Data; Overnight Results Invalid!!!")
                CloseGainPerc = 0.0
                dailyReturn = 0.0
            if self.VERBOSE:
                if PostTriggerVolumes:
                    PostTriggerMeanVolume = int(statistics.mean(PostTriggerVolumes))
                    PostTriggerMedianVolume = int(statistics.median(PostTriggerVolumes))
                    print("Post Trigger Median Volume: %d"%PostTriggerMedianVolume)
                    print("Post Trigger Average Volume: %d"%PostTriggerMeanVolume)
                if len(OpenClose)==2:
                    print("Open/Close: %.2f/%.2f"%(OpenClose[0],OpenClose[1]))
                #if "FILTER_SELLS" in props:
                #    print("Day's Volume Triggers")
                #    print(self.VOLUME_TRACKER[localSymbol])
                PreTriggerHigh = self.MAX_DAILY_PRICE[localSymbol]
                PreTriggerLow = self.MIN_DAILY_PRICE[localSymbol]
                category = int(triggerClose*10)*10
                #lineToWrite = "Date,P/C,Grp,MktGap,Open,High,Low,Trigger,Time,Loss2Profit,MaxProfit,%s,Volume,AvgVolume,Trades,AvgTrades,High,AvgHigh,Low,AvgLow,PostVolumeAvg,PostTradesAvg,daysTillExpiration,sameDayAvailable\n"%self.STRATEGY
                lineToWrite = "%s,%s,%d,%.2f%%,%.2f,%.2f,%.2f,%.2f,%s,%.2f%%,%.2f%%,%.2f%%,%d,%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%d,%d,%d,%s\n"%(triggerTime,optionType,category,overNightGap*100,OpenClose[0],PreTriggerHigh,PreTriggerLow,triggerPrice,triggerTime.strftime("%H:%M"),MaxDrawperc,maxGainPercent,dailyReturn,TriggerRow['volume'],FiveMinSumDict['Volume']/5,TriggerRow['barCount'],FiveMinSumDict['Trades']/5,TriggerRow['high'],FiveMinSumDict['High']/5,TriggerRow['low'],FiveMinSumDict['Low']/5,PostTriggerSumDict['Volume']/5,PostTriggerSumDict['Trades']/5,self.daysTillExpiration,self.sameDayAvailable)
                self.observationFile.write(lineToWrite)
                self.observationFile.flush()
            self.Sum_of_closes = self.Sum_of_closes + CloseGainPerc
            MaxGainperc = "Max Gain: %.2f%%"%(maxGainPercent)
            MaxDrawperc = "Loss2Profit: %.2f%%"%(MaxDrawperc)
            CloseGainPerc = "At Close: %.2f%%"%CloseGainPerc
            CloseDrawPerc = "DrawDownToClose: %.2f%%"%CloseDrawPerc
            #dailyReturn = "Daily: %.2f%%"%dailyReturn
            return(purchasePrice,loss2Profit,maxGain,MaxGainperc,MaxDrawperc,CloseGainPerc,CloseDrawPerc,dailyReturn,sellTime)
        else:
            return None

    def AddValuesToDict(self, sumDict, Trades, High, Low, Volume, DolVol):
        #FiveMinSumDict = {"Trades":0,"High":0,"Low":0,"Volume":0,"DolVol":0}
        sumDict["Trades"] = sumDict["Trades"] + int(Trades)
        sumDict["High"] = sumDict["High"] + float(High)
        sumDict["Low"] = sumDict["Low"] + float(Low)
        sumDict["Volume"] = sumDict["Volume"] + int(Volume)
        sumDict["DolVol"] = sumDict["DolVol"] + float(DolVol)
        return(sumDict)

    def getSellSignalsFromVolume(self, props, MinuteDataDict, optionType):
        sellSignalsDict = {}
        sellSignalsList = []
        if "EARLYEXITTRIGGER" in props:
            for localSymbol, df in MinuteDataDict.items():
                if self.INVERSE_OPTION_DICT[optionType] in localSymbol:  #Check to see if it is the opposite direction.
                #if optionType in localSymbol:                           #Same Direction Volume Spikes
                    try:
                        sellSignalsDict[localSymbol] = df[df.volume >= props["EARLYEXITTRIGGER"]]
                        for index, row in sellSignalsDict[localSymbol].iterrows():
                            #print(row['date'])
                            sellSignalsList.append(row['date'])
                    except Exception as detail:
                        print("No sell signals found")
        sellSignalsList.sort()
        return (sellSignalsList,sellSignalsDict)

    def getOptionDataFromCSV(self, underlying, expiration, contract_type, strike, date, nextTradeDate=None, interval=None):
        localSymbol = "%s%s%s%05d000"%(underlying.ljust(6),expiration[2:],contract_type,strike)
        Filename = "%s.csv"%(localSymbol)
        if interval and interval != "1min":
            Filename = "%s_%s.csv"%(localSymbol,interval)
        Filepath = "%s\OptionTraderBarData\%s"%(os.getenv('OPTIONTRADERALGOPATH'),underlying)  #New Google Drive Share Path
        if self.PolygonData:
            Filepath = "%s\%s\Polygon"%(os.getenv('OPTIONTRADERBARDATA'),underlying)  #New Google Drive Share Path
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
            return(localSymbol,filtered_df)
        else:
            print("Filepath doesn't exist: %s"%Filepath)
            return(None,None)

    def getPiloAlertsFromCSV(self, underlying, TimePeriod, date, nextTradeDate=None):
        filtered_df = None
        Filename = "%s_%s.csv"%(underlying,TimePeriod.lower())
        Filepath = "%s\OptionTraderBarData\%s"%(os.getenv('OPTIONTRADERALGOPATH'),underlying)  #New Google Drive Share Path
        #if self.PolygonData:
        #    Filepath = "%s\%s\Polygon"%(os.getenv('OPTIONTRADERBARDATA'),underlying)  #New Google Drive Share Path
        Filepath = os.path.join(Filepath,Filename)
        #print(Filepath)
        #print(" ")
        if TimePeriod not in self.df:
            print("Loading TimePeriod")
            if os.path.exists(Filepath):
                self.df[TimePeriod] = pd.read_csv(Filepath)
                #date = datetime(int(date.split("-")[0]), int(date.split("-")[1]), int(date.split("-")[2])).date()
                self.df[TimePeriod]['date'] = pd.to_datetime(self.df[TimePeriod]['date'], format='%Y_%m_%d_%H:%M:%S')
                #Round the dates to the nearest 15 seconds#
                for i,row in self.df[TimePeriod].iterrows():
                    rounded_date = row['date'] + timedelta(seconds=7)
                    rounded_date = rounded_date - timedelta(seconds=rounded_date.second % 15)
                    self.df[TimePeriod].at[i,'date'] = rounded_date
                if self.OVERNIGHT:
                    self.df[TimePeriod] = self.df[TimePeriod].set_index('date')
                    self.df[TimePeriod].index = pd.to_datetime(self.df[TimePeriod].index)
                    self.df[TimePeriod] = self.df[TimePeriod].sort_index()
            else:
                print("Filepath doesn't exist: %s"%Filepath)
                return pd.DataFrame()                         #return empty dataframe
        #Round the dates to the nearest 15 seconds#
        if self.OVERNIGHT:
            filtered_df = self.df[TimePeriod].loc[date.strftime("%Y-%m-%d"):nextTradeDate.strftime("%Y-%m-%d")].reset_index()
        else:
            filtered_df = self.df[TimePeriod].loc[self.df[TimePeriod]['date'].dt.strftime('%Y-%m-%d') == date.strftime("%Y-%m-%d")].reset_index()
        #print(filtered_df)
        return(filtered_df)
        

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-qqq", "--qqq", help="Run Backtester for QQQ", action="store_true")
    parser.add_argument("-spy", "--spy", help="Run Backtester for SPY", action="store_true")
    parser.add_argument("-tsla", "--tsla", help="Run Backtester for TSLA", action="store_true")
    parser.add_argument("-verbose", "--verbose", help="print additional information for research", action="store_true")
    parser.add_argument("-polygon", "--polygon", help="Use Polygon.io Historical Data", action="store_true")
    parser.add_argument("-cycle", "--cycle", help="cycle through variables and save data", action="store_true")
    parser.add_argument("-chunk", "--chunk", help="Used in combination with cycle to break computation into 3 chunk sizes")
    parser.add_argument("-strategy", "--strategy", help="Can force a different strategy to run besides default.")
    parser.add_argument("-symbol", "--symbol", help="Pass in a symbol")
    parser.add_argument("-interval", "--interval", help="Pass in the desired interval with which to operate on.")
    args = parser.parse_args()
    interval = "1min"
    underlying = "QQQ"
    strategy = "strategy1"
    if args.qqq:
        underlying = "QQQ"
    if args.spy:
        underlying = "SPY"
    if args.tsla:
        underlying = "TSLA"
    if args.symbol:
        underlying = args.symbol

    if args.strategy:
        strategy = args.strategy
    
    if args.interval:
        interval = args.interval

    backtest = BackTestOptionMomentum(strategy, args.polygon, args.verbose)
    backtest.main(underlying, args.cycle, args.chunk, interval)
    #lp = LineProfiler()
    #lp_wrapper = lp(backtest.main)
    #lp_wrapper(underlying, args.cycle, args.chunk)
    #lp.print_stats()