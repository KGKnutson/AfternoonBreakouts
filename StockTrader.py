from ib_insync import *
from datetime import *
from IBDConnParams import *
from HostTracker import *
import pandas_market_calendars as mcal
import pandas as pd
import time
import os
import sys
import traceback 
import math
import numpy as np
import Communicator
import OrderManager
import StockProps
import socket
import logging
import zerorpc

from BackTestBreakouts import BackTestBreakouts

class StockTrader(object):
    STRATEGY = None
    SLEEP_DELAY = 30
    MAXBUYS     = 5
    PUTCALLIDX = 12
    ACCOUNT = "PAPER"   #PAPER OR CASH#
    SELF_IP = "127.0.0.1"
    AFTERHOURSDEBUG = False
    today = datetime.now()   # - timedelta(days=2)
    OPTIONOFFSETLIST = [-5, 0, 5]
    IAMIBDHOST = False
    IBDATAHOST = None
    DEFAULT_ORDER_TYPE = "LMT"

    def __init__(self, strategy="AfternoonBreakouts"):
        try:
            self.STRATEGY = strategy
            logFile = os.path.join(os.environ.get("OPTIONTRADERLOGPATH"),"%s_%s.txt"%(strategy,datetime.now().strftime('%Y%m%d')))
            logging.basicConfig(filename=logFile, filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            logging.getLogger().setLevel(logging.INFO)
            self.ib = IB()
            self.com = Communicator.Communicator()
            self.connParam = IBDConnParams()
            self.sp = StockProps.StockProps()
            self.hostTracker = HostTracker(self.connParam, strategy)
            self.orderMgr = OrderManager.OrderManager(self.ib,self.com,strategy,self.connParam,self.today,self.AFTERHOURSDEBUG)
            self.BTStrategy = BackTestBreakouts(strategy)
            self.ib.connect(self.SELF_IP, self.connParam.getConnPort(self.ACCOUNT), clientId=self.connParam.getclientID(self.STRATEGY))  #Local Trader Workstation Connection
            nyse = mcal.get_calendar('NYSE')
            strToday = self.today.strftime('%Y-%m-%d')
            strNxtWeek = (self.today + timedelta(days=7)).strftime('%Y-%m-%d')
            #if self.AFTERHOURSDEBUG:
            #    today = "2022-04-29"
            self.marketHoursToday = nyse.schedule(strToday,strToday)
            self.marketHoursWeek = nyse.schedule(strToday,strNxtWeek)
            self.marketWeeklySchedule = []
            for row in self.marketHoursWeek.iterrows():
                self.marketWeeklySchedule.append(row[0].strftime("%Y%m%d"))
            self.market_close = self.market_open = None
            if not self.marketHoursToday.empty:
                self.market_close = datetime.fromtimestamp(self.marketHoursToday.market_close.item().timestamp())
                self.market_open = datetime.fromtimestamp(self.marketHoursToday.market_open.item().timestamp()) #datetime.today().replace(hour=6, minute=30, second=00, microsecond=00)
            print("market end time today: %s"%self.market_close)
            self.IBDATAHOST = self.hostTracker.getDataHostName()
            self.rpcHost = zerorpc.Client(timeout=5)
            if self.hostTracker.IAMIBHostProgram():
                self.IAMIBDHOST = True
            try:
                self.rpcHost.connect("tcp://%s:4242"%self.hostTracker.getDataHostIPAddress())
            except Exception as detail:
                print(detail)
                print("Failed to connect to RPCData Server.  This is probably because service is not running on defined host system: %s"%self.IBDATAHOST.upper())
                if not self.IAMIBDHOST:
                    raise Exception("Cannot run as we won't be able to get data from the RPC Data Server since we failed to connect")
        except Exception as detail:
            logging.critical(traceback.format_exc())
            logging.critical(detail)
            self.com.sendMsgToQueue("Program Terminated; Failed during %s init on %s!"%(self.STRATEGY, socket.gethostname()), contactList="SUPPORT")
            print(detail)
            print(traceback.format_exc())
            raise Exception("Critical fail during %s initialization"%self.STRATEGY)

    def getUnderlyingMarketData(self, underlying, openTimeSeconds):
        try:
            success = False
            df = todayDF = yesterdayDF = None
            while not success:
                try:
                    if self.IAMIBDHOST:
                        EquityContract = Stock(symbol=underlying, exchange='SMART', currency='USD')
                        self.ib.qualifyContracts(EquityContract)
                        end_date = datetime.now().strftime('%Y%m%d %H:%M:%S')
                        bars = self.ib.reqHistoricalData(EquityContract, endDateTime=end_date, durationStr='1 D', barSizeSetting='1 min', whatToShow='TRADES', useRTH=True)
                        df = util.df(bars)
                        try:
                            if df is None:
                                self.rpcHost.setDataUsingKey(underlying, None, datetime.now().strftime('%Y%m%d %H:%M:%S'), underlying)
                            else:
                                self.rpcHost.setDataUsingKey(underlying, df.to_json(orient='split'), datetime.now().strftime('%Y%m%d %H:%M:%S'), underlying)
                        except Exception as detail:
                            print(detail)
                            print("Failed to send data to the RPC host service.  This is probably because service is not running on defined host system: %s"%self.IBDATAHOST.upper())
                    else:
                        df = self.getDFfromRPCData(underlying, openTimeSeconds, getUnderlyingMarketData=True)
                    todayDF = df.iloc[0]
                    success = True
                except Exception as detail:
                    self.ib.disconnect()
                    print(detail)
                    print("Exception thrown trying to get Historical Underlying Data; sleeping 10 seconds")
                    print(traceback.format_exc())
                    logging.critical("Exception thrown trying to get underlying data; sleeping 10s")
                    #self.com.sendMsgToQueue("Exception getting Underlying Data; sleeping 10s", contactList="SUPPORT") #This will be alerted through heartbeat instead.
                    time.sleep(10)
                    try:
                        self.ib.connect(socket.gethostbyname(socket.gethostname()), self.connParam.getConnPort(self.ACCOUNT), clientId=self.connParam.getclientID(self.STRATEGY))  #Local Trader Workstation Connection
                    except Exception as detail:
                        print(detail)
                        print("Exception thrown trying to reconnect to IB program")
                    
            return(todayDF)

        except Exception as detail:
            logging.critical(traceback.format_exc())
            logging.critical(detail)
            self.com.sendMsgToQueue("%s on %s Program Terminated; Failed getting underlying market data!"%(self.STRATEGY,socket.gethostname()), contactList="SUPPORT")
            print(detail)

    def getDFfromRPCData(self, symbol, openSeconds, getUnderlyingMarketData=False):
        try:
            success = False
            retryCount = 0
            MAXRETRIES = 30
            df = None
            latency_accommodation = 2 #This is to accommodate slight clock discrepancies.
            openSeconds = openSeconds - latency_accommodation
            now = (self.market_open + timedelta(minutes=openSeconds/60))                 #Set our query time to what we expect (every 30 seconds)
            while not success and retryCount < MAXRETRIES:
                try:
                    jsonString, updateTime = self.rpcHost.getDataUsingKey(symbol)
                    #Used to get underlying (QQQ) df data#
                    while getUnderlyingMarketData and jsonString is None and retryCount <= MAXRETRIES:
                        time.sleep(1)
                        retryCount = retryCount + 1
                        jsonString, updateTime = self.rpcHost.getDataUsingKey(symbol)

                    #Used to get most recent option contract data#
                    while not getUnderlyingMarketData and datetime.strptime(updateTime, '%Y%m%d %H:%M:%S').time() < now.time():  #Check to make sure that we have the update we expect 9:30:00, 9:30:30, 9:31:00, etc.
                        time.sleep(1)
                        jsonString, updateTime = self.rpcHost.getDataUsingKey(symbol)
                        retryCount = retryCount + 1

                    if jsonString is not None:
                        jsonData = json.loads(jsonString)
                        df = pd.DataFrame(jsonData['data'], columns=jsonData['columns'])
                        df['date'] = pd.to_datetime(df['date'],unit='ms')
                        day = df[-1:]['date'].item()
                    success = True
                except Exception as detail:
                    retryCount = retryCount + 1
                    print(detail)
                    print("Exception thrown getting Option Data from RPCHost; sleeping 1 seconds")
                    print(traceback.format_exc())
                    time.sleep(1)
            if retryCount >= MAXRETRIES:
                logging.critical("Could not get option or underlying data from RPC Host on %s after %d retries; getUnderlying: %s"%(socket.gethostname(),MAXRETRIES, getUnderlyingMarketData))
                return(df)   #Just return what we have and it can error in main#
            return(df)

        except Exception as detail:
            logging.critical(traceback.format_exc())
            logging.critical(detail)
            self.com.sendMsgToQueue("%s on %s Program Terminated; Failed getting market data from RPC Host!"%(self.STRATEGY,socket.gethostname()), contactList="SUPPORT")
            print(detail)

    def sleep(self):
        now = datetime.now()
        sleep_time = self.SLEEP_DELAY - now.second%self.SLEEP_DELAY
        SleepTillTime = now + timedelta(seconds=sleep_time)
        SleepTillTime = SleepTillTime.replace(microsecond=0)  #This corrects for any offset within the minute we may have#
        time_to_sleep = (SleepTillTime - now).total_seconds()
        if time_to_sleep > 0:
            print("Sleeping %d seconds"%time_to_sleep)
            time.sleep(time_to_sleep)
        else:
            print("Didn't sleep")
        return self.SLEEP_DELAY

    def reloadSettingsFromMemory(self, orderDict):
        buyDict = {}
        ContractsDict = {}
        minuteDataDict = {}
        buysProcessed = 0
        lastBuyTime = False
        for submittedTime, properties in orderDict.items():
            triggerTime = datetime.strptime(properties["TriggerTime"],'%Y%m%d %H:%M:%S')
            submittedTime = datetime.strptime(properties["SubmittedDateTime"],'%Y%m%d %H:%M:%S')
            symbol = properties["localSymbol"]
            buyDict[properties["localSymbol"]] = {"TRIGGERINDEX": properties["TriggerIndex"],
                        "TRIGGERTIME":  triggerTime,
                        "ORDERSUBMISSIONTIME": submittedTime,
                        "LOCALSYMBOL":  properties["localSymbol"],
                        "ENTRYLIMIT":   properties["LimitPrice"],
                        "TARGETLIMIT":  properties["TargetLimit"], 
                        "STOPLIMIT":    properties["StopLimit"],
                        "PROPS":        properties["Props"],
                        "STATUS":       properties["Status"]
                        }
            try:
                EquityContract = Stock(symbol=symbol, exchange='SMART', currency='USD')
                self.ib.qualifyContracts(EquityContract)
                ContractsDict[symbol] = EquityContract
                #minuteDataDict[symbol] = self.getUnderlyingMarketData(symbol, openTimeSeconds)
            except Exception as detail:
                print("Exception thrown trying to restore dictionary")
                print(traceback.format_exc())
                logging.critical(traceback.format_exc())
                logging.critical(detail)
                logging.critical("Exception thrown trying to restore Contract and Minutedata from saved dictionary.")
            if properties["Status"] in ["SUBMITTED","ACQUIRED","SOLD"]:  #We ignore expired and cancelled orders
                if buysProcessed==0 and properties["Props"]["MAXDAILYBUYS"] > self.MAXBUYS:
                    self.MAXBUYS = properties["Props"]["MAXDAILYBUYS"]
                buysProcessed = buysProcessed + 1
                lastBuyTime = properties["Props"].get("LAST_BUY_TIME")
                if lastBuyTime:
                    lastBuyTime = datetime.strptime(lastBuyTime,"%H:%M:%S")
                    lastBuyTime = datetime.combine(datetime.now().date(), lastBuyTime.time())
        return lastBuyTime, buyDict, ContractsDict, minuteDataDict

    def cleanUpBuyDict(self, underlying, buyDict, ContractsDict, minuteDataDict, currentTime, piloAlertsDict, lastBuyTime):
        openPositions = 0
        removeList = []
        openOrders = ["SUBMITTED","ACQUIRED","EXPIRED"]
        for symbol,triggerDict in buyDict.items():
            if triggerDict["STATUS"] in openOrders:  #Check for an update on these positions as we consider them to still be open.
                BuyOptionSymbol = triggerDict["LOCALSYMBOL"]
                submittedTime = triggerDict["ORDERSUBMISSIONTIME"]
                try:
                    logging.info("IB Checking Position Status Request")
                    triggerDict["STATUS"] = self.orderMgr.checkPositionStatus(ContractsDict[BuyOptionSymbol],minuteDataDict[BuyOptionSymbol],currentTime,submittedTime)
                    logging.info("IB Checking Position Status Complete")
                except Exception as detail:
                    logging.critical("Exception thrown checking position statuses! No Bueno!!")
                    logging.critical(traceback.format_exc())
                    logging.critical(detail)
            if triggerDict["STATUS"] in openOrders:  #We consider positions open until "SOLD" or "CANCELLED"
                openPositions = openPositions + 1
                try:
                    if "EODSELLMINUTESFROMCLOSE" in triggerDict["PROPS"]:
                        exitPositionTime = int(triggerDict["PROPS"]["EODSELLMINUTESFROMCLOSE"])
                        if (currentTime + timedelta(minutes=1)) >= (self.market_close - timedelta(minutes=exitPositionTime)): #Check if we need to sell the position
                            self.orderMgr.closeOpenPositions(minuteDataDict, currentTime, BuyOptionSymbol)  #UNLOAD OPEN POSITIONS#
                except Exception as detail:
                    logging.critical("Exception thrown trying to close position %s!!"%BuyOptionSymbol)
                    logging.critical(traceback.format_exc())
                    logging.critical(detail)
            if triggerDict["STATUS"] in ["CANCELLED"]:  #We Remove this from the buyDict so it is not counted towards
                removeList.append(BuyOptionSymbol)
        return openPositions, removeList

    #def getPiloTradesDataFrames(self, underlying, TimePeriods_List):
    #    alertsDict = {}
    #    for timePeriod in TimePeriods_List:
    #        alerts = self.rpcHost.getPiloTradeAlertsForKey("%s_%s"%(underlying.upper(),timePeriod.lower()))
    #        df = pd.DataFrame(alerts.items(), columns=['date', 'action'])
    #        df['date'] = pd.to_datetime(df['date'], format="%Y_%m_%d_%H:%M:%S")
    #        #Round the dates to the nearest 15 seconds#
    #        for i,row in df.iterrows():
    #            rounded_date = row['date'] + timedelta(seconds=7)
    #            rounded_date = rounded_date - timedelta(seconds=rounded_date.second % 15)
    #            df.at[i,'date'] = rounded_date
    #        alertsDict[timePeriod] = df
    #    #print(alertsDict)
    #    return alertsDict

    def SaveDaysResultsFile(self):
        resultsFile = os.path.join(os.environ.get("OPTIONTRADERLOGPATH"),"%s.csv"%(self.STRATEGY))
        #Write header if file doesn't exist
        if not os.path.exists(resultsFile):
            with open(resultsFile, 'w') as f:
                f.write("Date,NumberOfBuys,Return\n")
        
        #Get orderDict and compute results
        orderDict = self.orderMgr.getOrderDict()
        BuyQty = 0
        Return = 0
        for submissionTime,attributeDict in orderDict.items():
            if "OpenPrice" in attributeDict and "ClosedPrice" in attributeDict:
                Return = Return + (attributeDict["ClosedPrice"] - attributeDict["OpenPrice"])/attributeDict["OpenPrice"]
            BuyQty = BuyQty + 1
        
        try:
            with open(resultsFile, 'a+') as f:
                f.write("%s,%d,%.2f\n"%(datetime.now().strftime("%Y-%m-%d"),BuyQty,Return))
        except Exception as detail:
            print(traceback.format_exc())
            logging.critical(traceback.format_exc())
            logging.critical(detail)
        try:
            self.com.sendMsgToQueue("%s:%s Day's Return: %.02f"%(socket.gethostname(),self.STRATEGY,Return), contactList="SUPPORT")
        except Exception as detail:
            print(traceback.format_exc())
            logging.critical(traceback.format_exc())
            logging.critical(detail)
            

    def main(self, underlying):
        if not self.marketHoursToday.empty:  #Check to make sure market open today.
            try:
                #Initialize our start time
                initStartTime = self.market_open + timedelta(seconds=10)
                loopStartTime = self.market_open + timedelta(seconds=60)
                openTimeSeconds = (initStartTime - self.market_open).seconds
                
                #Check for open Orders and reload from memory if there were outstanding orders from a previous run#
                orderDict = self.orderMgr.getOrderDict()
                lastBuyTime, buyDict, ContractsDict, minuteDataDict = self.reloadSettingsFromMemory(orderDict)

                #Set Current Time#
                currentTime = datetime.now()
                if self.AFTERHOURSDEBUG:
                        currentTime = currentTime.replace(hour=12, minute=4, second=00, microsecond=00)
                #if self.AFTERHOURSDEBUG:  #Used to debug a previous day's behavior#
                #    currentTime = currentTime - timedelta(days=1)
                if currentTime < initStartTime:
                    print("Sleeping till we hit initStartTime: %s"%initStartTime.strftime("%H:%M:%S"))
                    time.sleep((initStartTime - currentTime).seconds)
                    currentTime = initStartTime

                firstLoop       = True
                openPositions   = 0
                maxBuys         = self.MAXBUYS
                PiloAlertsDataFrameDict = {}
                props           = None
                CancelledDict   = {}

                if not self.AFTERHOURSDEBUG:
                    currentTime = datetime.now()
                if currentTime < loopStartTime:
                    print("Sleeping till we are ready to loop: %s"%loopStartTime.strftime("%H:%M:%S"))
                    time.sleep((loopStartTime - currentTime).seconds)
                    openTimeSeconds = 0
                    currentTime = self.market_open
                else:
                    if self.SLEEP_DELAY == 15:
                        currentTime = currentTime.replace(microsecond=00) - timedelta(seconds=15)
                    else:
                        currentTime = currentTime.replace(second=00, microsecond=00) - timedelta(minutes=1)
                    openTimeSeconds = (currentTime - self.market_open).seconds
                while currentTime <= self.market_close:             #option Market closes @ 1:14, but we want to stop after 1:00 and datetime.now() is 1 min ahead of currentTime 
                    print(currentTime)
                    logging.info("Top of the currentTime Loop")
                    end_date = (currentTime + timedelta(minutes=1)).strftime('%Y%m%d %H:%M:%S')  #currentTime.strftime('%Y%m%d %H:%M:%S')
                    success = False
                    
                    if not firstLoop and (int(currentTime.second+self.SLEEP_DELAY)%(self.SLEEP_DELAY*4)!=0) and self.SLEEP_DELAY<30:  #only pull data every 0 and 30 seconds
                        success = True
                        logging.info("Skipping IB Data Pull")
                        print("Skipping IB Data pull")
                    while not success and (datetime.now() <= self.market_close or self.AFTERHOURSDEBUG):
                        try:
                            print("Pulling IB Data")
                            logging.info("Pulling IB Data")
                            RPCServiceUp = True
                            for symbol,Contract in ContractsDict.items():
                                df = None
                                if self.IAMIBDHOST:
                                    bars = self.ib.reqHistoricalData(Contract, endDateTime=end_date, durationStr='1 D', barSizeSetting='1 min', whatToShow='TRADES', useRTH=True)
                                    df = util.df(bars)
                                    if RPCServiceUp:
                                        try:
                                            if df is None:
                                                self.rpcHost.setDataUsingKey(symbol, None, datetime.now().strftime('%Y%m%d %H:%M:%S'), self.STRATEGY)
                                            else:
                                                self.rpcHost.setDataUsingKey(symbol, df.to_json(orient='split'), datetime.now().strftime('%Y%m%d %H:%M:%S'), self.STRATEGY)
                                        except Exception as detail:
                                            RPCServiceUp = False
                                            print(detail)
                                            print("Failed to send data to the RPC host service.  This is probably because service is not running on defined host system: %s"%self.IBDATAHOST.upper())
                                else:
                                    df = self.getDFfromRPCData(symbol, openTimeSeconds)
                                if df is not None:
                                    minuteDataDict[symbol] = df
                                    day = df[-1:]['date'].item()
                                    #print(df)
                            success = True
                        except Exception as detail:
                            self.ib.disconnect()
                            print(detail)
                            print("Exception thrown trying to get Historical Option Data; sleeping 15 seconds")
                            print(traceback.format_exc())
                            logging.critical("Exception getting Option Data; sleeping 30s")
                            logging.critical(traceback.format_exc())
                            logging.critical(detail)
                            #self.com.sendMsgToQueue("Exception getting Option Data; sleeping 20s", contactList="SUPPORT")
                            time.sleep(15)
                            try:
                                self.ib.connect(socket.gethostbyname(socket.gethostname()), self.connParam.getConnPort(self.ACCOUNT), clientId=self.connParam.getclientID(self.STRATEGY))  #Local Trader Workstation Connection
                            except Exception as detail:
                                print(detail)
                                print("Exception thrown trying to reconnect to IB program")
                    logging.info("Done with IB Data Loop")
                    #TODO: if not success, we need to liquidate any open positions; We have an unreliable connection.

                    #Look for Buy Triggers and and Submit Buy Orders
                    logging.info("Clean Up Buy Dict")
                    openPositions, removeList = self.cleanUpBuyDict(underlying, buyDict,ContractsDict,minuteDataDict,currentTime,PiloAlertsDataFrameDict,lastBuyTime)
                    for symbol in removeList:
                        ContractsDict.pop(symbol)
                        buyDict.pop(symbol)                   #Remove the item from our buyDict so we can begin looking for another opportunity
                        if symbol in CancelledDict:
                            CancelledDict[symbol] = CancelledDict[symbol] + 1
                        else:
                            CancelledDict[symbol] = 1
                        if len(buyDict) == 0:
                            maxBuys = self.MAXBUYS  
                    print("openPositions: %d; Number of equity Contracts: %d"%(openPositions, len(minuteDataDict)))
                    logging.info("Clean Up Complete")

                    #Get current properties
                    triggerIndex = int((currentTime - self.market_open).seconds/60)
                    props = self.sp.getTriggerProperties("AfternoonBreakouts",triggerIndex)
                    
                    #OVERRIDE DEFAULT MAX BUYS IF RULES FILE HAS DEFINITION
                    if len(buyDict) == 0:
                        maxBuys = props.get("MAXDAILYBUYS", self.MAXBUYS)

                    #Check for a buy signal
                    if len(buyDict) < maxBuys:
                        if len(buyDict)>0:
                            print("Looking for buy signal again; Buy Dict length %d"%len(buyDict))
                        #overRideProps=None
                        #BuyOptionSymbol, HighVoteVolume, HighVoteDollarVolume, triggerPrice, triggerTime, props, piloAlertTime, lastBuyTime = self.BTStrategy.checkForBuySignal(minuteDataDict, forceFutureOptionType, currentTime, self.market_open, self.market_close, overnightGapPercent, len(buyDict), PiloAlertsDataFrameDict, overRideProps, piloTriggerOffset, optionDataOffset)
                        alertsDict = self.rpcHost.getAfternoonBOAlerts()
                        print(alertsDict)
                        for symbol,attrs in alertsDict.items():
                            if symbol not in buyDict.keys() and len(buyDict) < maxBuys:
                                #This currently will round purchasePrice down to the lowest .01 dollar
                                purchasePrice, targetPrice, stopPrice = self.BTStrategy.getLimitPrices(attrs["lmtPrice"], props, overNightGap=None)
                                ########################################################
                                orderSubmissionTime = datetime.today().replace(microsecond=00)
                                if self.AFTERHOURSDEBUG:
                                    orderSubmissionTime = currentTime + timedelta(seconds=self.SLEEP_DELAY)
                                buyDict[symbol] = {"TRIGGERINDEX": triggerIndex, 
                                                        "TRIGGERTIME":  currentTime,
                                                        "ORDERSUBMISSIONTIME": orderSubmissionTime,
                                                        "BREAKOUTTIME": attrs["breakoutTime"],
                                                        "LOCALSYMBOL":  symbol,
                                                        "ENTRYLIMIT":   float(attrs["lmtPrice"]),
                                                        "TARGETLIMIT":  targetPrice, 
                                                        "STOPLIMIT":    stopPrice,
                                                        "PROPS":        props,
                                                        "STATUS":       "SUBMITTED"
                                                        }
                                #print(buyDict[symbol]["ENTRYLIMIT"])
                                #buyList.append(buyDict)
                                orderType = self.DEFAULT_ORDER_TYPE
                                try:
                                    if "ORDER_TYPE" in props:
                                        orderType = props["ORDER_TYPE"]   #We subtract 1 from the buyDict because our list is "0" indexed
                                except Exception as detail:
                                    logging.critical(traceback.format_exc())
                                    logging.critical(detail)
                                    logging.critical("Failed to set orderType for Buy order")
                                    
                                logging.info("Submit Order")
                                try:
                                    if symbol not in CancelledDict or CancelledDict[symbol] < 2:  #Only make 2 attempts to purchase equity
                                        EquityContract = Stock(symbol=symbol, exchange='SMART', currency='USD')
                                        self.ib.qualifyContracts(EquityContract)
                                        ContractsDict[symbol] = EquityContract
                                        self.orderMgr.submitBuyOrder(ContractsDict[symbol], buyDict[symbol], orderType)
                                        #Pull DF Data
                                        minuteDataDict[symbol] = self.getUnderlyingMarketData(symbol, openTimeSeconds)
                                        #logging.info("Check Position Status")
                                        #status = self.orderMgr.checkPositionStatus(ContractsDict[symbol],minuteDataDict[symbol],currentTime,orderSubmissionTime)
                                except Exception as detail:
                                    logging.critical(traceback.format_exc())
                                    logging.critical(detail)
    
                    #Verify that all positions are flat at market close 1:00
                    logging.info("Close opens at the end of the day")
                    if (currentTime + timedelta(minutes=1)) >= self.market_close:
                        try:
                            self.orderMgr.closeOpenPositions(minuteDataDict, currentTime)  #UNLOAD OPEN POSITIONS#
                            self.orderMgr.endOfDayPositionChecker()
                        except Exception as detail:
                            logging.critical(traceback.format_exc())
                            logging.critical(detail)
                            self.com.sendMsgToQueue("%s:%s Crashed trying to run EOD validation. We might still have a position!"%(socket.gethostname(),self.STRATEGY), contactList="SUPPORT")
                            print(detail)

                    try:
                        logging.info("Notify I'm Alive")
                        self.rpcHost.notifyIMAlive("%s_%s"%(self.STRATEGY,socket.gethostname().lower()))      #Notify Server that this machine and program is alive
                    except:
                        print("RPCHost must be down")
                    #Sleep until next minute interval
                    firstLoop = False
                    logging.info("Sleep till next loop")
                    openTimeSeconds = openTimeSeconds + self.sleep()

                    if self.AFTERHOURSDEBUG:
                        currentTime = self.market_open + timedelta(minutes=openTimeSeconds//60)  #Round down to last minute
                    else:
                        currentTime = datetime.today().replace(second=00, microsecond=00) - timedelta(minutes=1)
                        openTimeSeconds = (datetime.today() - self.market_open).seconds               #Re-calculate open time seconds
                    logging.info("Loop Finished")
                print("Buy Dict"%buyDict)
                self.SaveDaysResultsFile()
                self.ib.disconnect()
                time.sleep(3)
            except Exception as detail:
                logging.critical(traceback.format_exc())
                logging.critical(detail)
                self.com.sendMsgToQueue("%s Program Terminated; Failed while running main program on %s!"%(self.STRATEGY,socket.gethostname()), contactList="SUPPORT")
                print(detail)

if __name__ == "__main__":
    import argparse
    #stockengine = stocks()
    parser = argparse.ArgumentParser()
    parser.add_argument("-qqq", "--qqq", help="Save Option Minute Data for QQQ", action="store_true")
    parser.add_argument("-strategy", "--strategy", help="Can force a different strategy to run besides default.")
    parser.add_argument("-symbol", "--symbol", help="Pass in a symbol")
    args = parser.parse_args()
    underlying = "QQQ"
    strategy = "afternoonbreakouts"
    if args.qqq:
        underlying = "QQQ"
        strategy = "strategy1"
    if args.symbol:
        underlying = args.symbol

    if args.strategy:
        strategy = args.strategy
    
    trader = StockTrader(strategy)
    trader.main(underlying.upper())
    #trader.SaveDaysResultsFile()
    #trader.ib.disconnect()
    #time.sleep(3)