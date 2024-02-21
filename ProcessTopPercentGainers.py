from datetime import *
from IBDConnParams import *
from HostTracker import *
from GetStockRank import GetStockRank
import pandas_market_calendars as mcal
import pandas as pd
import Afternoon_Breakouts
import Communicator
import time
import os
import sys
import traceback 
import socket
import logging
import zerorpc

class ProcessTopPercentGainers(object):
    GainersDict = {}
    STRATEGY = "afternoonbreakouts"
    SLEEP_DELAY = 60
    MAXBUYS     = 10
    CONSOLIDATION_TIME = 45
    dHOD_ORDER_LMT = 0.10
    AFTERHOURSDEBUG = False
    today = datetime.now()   # - timedelta(days=2)

    def __init__(self):
        try:
            logFile = os.path.join(os.environ.get("AB_LOGPATH"),"ABLogfile_%s.txt"%(datetime.now().strftime('%Y%m%d')))
            logging.basicConfig(filename=logFile, filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            #logging.getLogger().setLevel(logging.INFO)
            self.gsr = GetStockRank()
            self.com = Communicator.Communicator()
            self.connParam = IBDConnParams()
            self.hostTracker = HostTracker(self.connParam, strategy=self.STRATEGY)
            self.ab_screener = Afternoon_Breakouts.screener(self.connParam.getclientID(self.STRATEGY)+1)
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
            #self.com.sendMsgToQueue("Program Terminated; Failed during %s init on %s!"%(self.STRATEGY, socket.gethostname()), contactList="SUPPORT")
            print(detail)
            print(traceback.format_exc())
            raise Exception("Critical fail during Afternoon_Breakout() initialization")
        
    def main(self):
        if not self.marketHoursToday.empty:  #Check to make sure market open today.
            try:
                #Initialize our start time
                initStartTime = self.market_open - timedelta(seconds=60)
                loopStartTime = self.market_open
                openTimeSeconds = 0
                
                #Set Current Time#
                currentTime = datetime.now()
                if self.AFTERHOURSDEBUG:
                        currentTime = currentTime.replace(hour=7, minute=29, second=00, microsecond=00)
                #if self.AFTERHOURSDEBUG:  #Used to debug a previous day's behavior#
                #    currentTime = currentTime - timedelta(days=1)
                if currentTime < initStartTime:
                    print("Sleeping till we hit initStartTime: %s"%initStartTime.strftime("%H:%M:%S"))
                    time.sleep((initStartTime - currentTime).seconds)
                    currentTime = initStartTime
                    
                #Spot reserved if we need to initialize anything

                if not self.AFTERHOURSDEBUG:
                    currentTime = datetime.now()
                if currentTime < loopStartTime:
                    print("Sleeping till we are ready to loop: %s"%loopStartTime.strftime("%H:%M:%S"))
                    time.sleep((loopStartTime - currentTime).seconds)
                    openTimeSeconds = 0
                    currentTime = self.market_open
                
                if currentTime > self.market_open + timedelta(minutes=1):  #We need to pull histories to populate our dict
                    self.preload_GainersDict(currentTime)
                else:
                    self.Update_GainersDict()
                    #TODO: We need a function that pulls the history and sets the correct tHOD in our dict

                firstLoop = True
                while currentTime <= self.market_close:             #option Market closes @ 1:14, but we want to stop after 1:00 and datetime.now() is 1 min ahead of currentTime 
                    if not firstLoop:
                        self.Update_GainersDict()
                    for symbol,attrs in self.GainersDict.items():
                        if (currentTime - attrs["tHOD"]).total_seconds()/60 > self.CONSOLIDATION_TIME:
                            attrs["Buyable"] = "True"
                        else:
                            attrs["Buyable"] = "False"

                    #print a clean output of the current items being monitored
                    df = pd.DataFrame.from_dict(self.GainersDict, orient='index')
                    df = df.sort_values(by='dHOD',ascending='True')
                    output = df.to_string(formatters={'dHOD': '{:,.2f}%'.format})
                    print(output)

                    try:
                        logging.info("Notify I'm Alive")
                        self.rpcHost.notifyIMAlive("%stracker_%s"%(self.STRATEGY,socket.gethostname().lower()))      #Notify Server that this machine and program is alive
                    except:
                        print("RPCHost must be down")
                    #Sleep until next minute interval
                    firstLoop = False
                    #logging.info("Sleep till next loop")
                    print("Completed Loop at %s"%datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    #openTimeSeconds = openTimeSeconds + self.sleep()

                    if self.AFTERHOURSDEBUG:
                        currentTime = self.market_open + timedelta(minutes=openTimeSeconds//60)  #Round down to minute
                    else:
                        currentTime = datetime.today().replace(second=00, microsecond=00)
                        openTimeSeconds = (datetime.today() - self.market_open).seconds               #Re-calculate open time seconds


            except Exception as detail:
                logging.critical(traceback.format_exc())
                logging.critical(detail)
                #self.com.sendMsgToQueue("%s Program Terminated; Failed while running main program on %s!"%(self.STRATEGY,socket.gethostname()), contactList="SUPPORT")
                print(detail)
                print(traceback.format_exc())
                
    def Update_GainersDict(self):
        FLOAT = 0
        PRICE = 1
        PREVCLOSE = 2
        DAILYHIGH = 3
        DAILYLOW = 4
        topGainers = self.ab_screener.get_top_percent_gainers_from_IB()
        updated_list = []
        #Loop through top gainers and update values
        for symbol,attrs in topGainers.items():
            updated_list.append(symbol)
            if symbol not in self.GainersDict.keys():
                print("Adding symbol %s"%symbol)
                self.GainersDict[symbol] = {"floatShares":attrs[FLOAT], 
                                                  "price":attrs[PRICE],
                                              "prevClose":attrs[PREVCLOSE],
                                              "dailyHigh":attrs[DAILYHIGH],
                                                   "tHOD":datetime.now(),
                                                   "Change":"%.2f%%"%((attrs[PRICE] - attrs[PREVCLOSE])*100/attrs[PREVCLOSE]),
                                                   "dHOD":float("%.2f"%((attrs[DAILYHIGH]-attrs[PRICE])*100/attrs[DAILYHIGH])),
                                                   "rank":int(self.gsr.getStockRank(attrs[FLOAT],attrs[FLOAT]*attrs[PRICE])),
                                              "alertSent":False
                                            }                
            else:
                if attrs[DAILYHIGH] > self.GainersDict[symbol]["dailyHigh"]:
                    #If we meet breakout criteria
                    if (datetime.now() - self.GainersDict[symbol]["tHOD"]).total_seconds()/60 > self.CONSOLIDATION_TIME and not self.GainersDict[symbol]["alertSent"]:
                        additional_attrs = {"prevClose":self.GainersDict[symbol]["prevClose"]}
                        self.rpcHost.setAfternoonBOAlert(symbol,self.GainersDict[symbol]["dailyHigh"],self.GainersDict[symbol]["rank"],additional_attrs)
                        self.GainersDict[symbol]["alertSent"] = True
                    self.GainersDict[symbol]["dailyHigh"] = attrs[DAILYHIGH]
                    self.GainersDict[symbol]["tHOD"]      = datetime.now()
                self.GainersDict[symbol]["price"] = attrs[PRICE]
                self.GainersDict[symbol]["dHOD"] = float("%.2f"%((attrs[DAILYHIGH]-attrs[PRICE])*100/attrs[DAILYHIGH]))
                self.GainersDict[symbol]["Change"] = "%.2f%%"%((attrs[PRICE] - attrs[PREVCLOSE])*100/attrs[PREVCLOSE])
        #Run this loop to update values for items that may have dropped off the top percent list
        for symbol,attrs in self.GainersDict.items():
            #print(symbol,attrs)
            if symbol not in updated_list:
                tickerDetails = self.ab_screener.getTickerDetails(symbol)
                self.GainersDict[symbol]["price"] = tickerDetails["regularMarketPrice"]
                dHOD = (tickerDetails["dailyHigh"]-tickerDetails["regularMarketPrice"])*100/tickerDetails["regularMarketPrice"]
                self.GainersDict[symbol]["dHOD"] = float("%.2f"%dHOD)

    def preload_GainersDict(self, currentTime=None):
        FLOAT = 0
        PRICE = 1
        PREVCLOSE = 2
        DAILYHIGH = 3
        DAILYLOW = 4
        THOD = 5
        topGainers = self.ab_screener.get_tHOD_from_IB_top_percent_scanner(currentTime)
        #print(topGainers)
        for symbol,attrs in topGainers.items():
            if symbol not in self.GainersDict.keys():
                self.GainersDict[symbol] = {"floatShares":attrs[FLOAT], 
                                                  "price":attrs[PRICE],
                                              "prevClose":attrs[PREVCLOSE],
                                              "dailyHigh":attrs[DAILYHIGH],
                                                   "tHOD":attrs[THOD],
                                                   "Change":"%.2f%%"%((attrs[PRICE] - attrs[PREVCLOSE])*100/attrs[PREVCLOSE]),
                                                   "dHOD":float("%.2f"%((attrs[DAILYHIGH]-attrs[PRICE])*100/attrs[DAILYHIGH])),
                                                   "rank":int(self.gsr.getStockRank(attrs[FLOAT],attrs[FLOAT]*attrs[PRICE])),
                                              "alertSent": False
                                            }

    def sleep(self):
        now = datetime.now()
        sleep_time = self.SLEEP_DELAY - now.second%self.SLEEP_DELAY
        SleepTillTime = now + timedelta(seconds=sleep_time)
        SleepTillTime = SleepTillTime.replace(microsecond=0)  #This corrects for any offset within the minute we may have#
        time_to_sleep = (SleepTillTime - now).total_seconds()
        if time_to_sleep > 0:
            print("Sleeping %d seconds %s"%(time_to_sleep,datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            time.sleep(time_to_sleep)
        else:
            print("Didn't sleep")
        return self.SLEEP_DELAY

if __name__ == "__main__":
    ptg = ProcessTopPercentGainers()
    ptg.main()
