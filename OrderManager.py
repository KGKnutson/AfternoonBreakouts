from ib_insync import *
from datetime import *
import time
import json
import os
import socket
import decimal

IB_ACCOUNT = 'DU4656963'

class OrderManager(object):
    data = {}
    orderIdTable = {}
    ib = None
    strategyID = None
    com = None
    INVESTMENT_SIZE = 0   #Default investment size; This is set in IBDConnParams.json
    TRADE_ENABLED = True
    SEND_ALERTS = True
    ActiveOrderFile = ""
    EODCheckComplete = False
    CONTACTGROUP = socket.gethostname().upper()

    def __init__(self, ib, com, strategyID, IBParams, today, AFTERHOURSDEBUG=None):
        #Read .xls content into program memory#
        self.ActiveOrderFile = os.path.join(os.environ.get("OPTIONTRADERORDERPATH"),"%s_%s.json"%(today.strftime('%Y%m%d'),strategyID))
        self.strategyID = strategyID
        self.ib = ib
        self.com = com
        self.INVESTMENT_SIZE = IBParams.getInvestmentSize(machineID=socket.gethostname(), strategy=strategyID) #Set investment size based on machine and strategy.
        print("Investment Size: $%.2f"%self.INVESTMENT_SIZE)
        if self.INVESTMENT_SIZE == 0:
            self.TRADE_ENABLED = False
            self.SEND_ALERTS = False
        if os.path.exists(self.ActiveOrderFile):
            with open(self.ActiveOrderFile, 'r') as f:
                self.data = json.load(f)
            for submittedTimeKey,tradeDict in self.data.items():
                for orderID in tradeDict.keys():
                    self.orderIdTable[orderID] = submittedTimeKey
        if AFTERHOURSDEBUG:
            self.CONTACTGROUP = "SUPPORT"

    def getOrderDict(self):
        return self.data

    def get_orderId_From_Type(self, orderIdDict, Type):
        for key, value in orderIdDict.items():
            if Type == value:
                return key

    def saveDataToFile(self):
        with open(self.ActiveOrderFile, 'w') as outfile:
            outfile.write(json.dumps(self.data))

    def submitBuyOrder(self, contract, buyDict, order_Type="LMT"):#  entryLimitPrice, targetLimitPrice, stopLimitPrice, triggerIndex):
        #SaveOrderToFile
        print(buyDict)
        positionSize = round(self.INVESTMENT_SIZE/(float(buyDict["ENTRYLIMIT"])))
        #positionSize = 1  #Hard-code to 1 for now
        status = "SUBMITTED"
        submittedTimeKey = buyDict["ORDERSUBMISSIONTIME"].strftime('%H:%M:%S')
        self.data[submittedTimeKey] = {"SubmittedDateTime": buyDict["ORDERSUBMISSIONTIME"].strftime('%Y%m%d %H:%M:%S'),
                                           "localSymbol": contract.localSymbol,
                                           "PositionSize": positionSize,
                                           "Status": status,                       #SUBMITTED  ACQUIRED   SOLD
                                           "LimitPrice": buyDict["ENTRYLIMIT"],
                                           "TargetLimit": buyDict["TARGETLIMIT"],
                                           "StopLimit": buyDict["STOPLIMIT"],
                                           "TriggerIndex": buyDict["TRIGGERINDEX"],
                                           "TriggerTime": buyDict["TRIGGERTIME"].strftime('%Y%m%d %H:%M:%S'),   #Have to convert this to string to save as json file
                                           "Expiration": (buyDict["TRIGGERTIME"]+timedelta(minutes=buyDict["PROPS"]["EXPIRATION"])).strftime('%Y%m%d %H:%M:%S'),
                                           "Props": buyDict["PROPS"],
                                           "BuyOrderId": "",
                                           "Orders": {},
                                           "OrderStatus": {}
                                           }
        if self.TRADE_ENABLED:
            #parent = Order(action='BUY', orderType="LMT", totalQuantity=positionSize, tif='DAY', lmtPrice=buyDict["ENTRYLIMIT"], account=IB_ACCOUNT, transmit=False)  #We may need account someday
            parent = Order(action='BUY', orderType=order_Type, totalQuantity=positionSize, tif='DAY', lmtPrice=buyDict["ENTRYLIMIT"], transmit=False)
            if order_Type=="LIT":
                parent = Order(action='BUY', orderType=order_Type, totalQuantity=positionSize, tif='DAY', lmtPrice=buyDict["ENTRYLIMIT"], auxPrice=buyDict["ENTRYLIMIT"], transmit=False)
            print(self.ib.placeOrder(contract, parent))
            profit = Order(parentId=parent.orderId, action='SELL', orderType="LMT", totalQuantity=positionSize, tif='DAY', lmtPrice=buyDict["TARGETLIMIT"], transmit=False)
            stp    = Order(parentId=parent.orderId, action='SELL', orderType="STP", totalQuantity=positionSize, tif='DAY', auxPrice=buyDict["STOPLIMIT"], transmit=True)
            for order in [profit, stp]:
                print(self.ib.placeOrder(contract, order))
            self.data[submittedTimeKey]["Orders"] = {str(parent.orderId):"BUY", str(profit.orderId):"PROFIT", str(stp.orderId):"STP"}
            self.orderIdTable[str(parent.orderId)] = self.orderIdTable[str(profit.orderId)] = self.orderIdTable[str(stp.orderId)] = submittedTimeKey
            self.data[submittedTimeKey]["BuyOrderId"] = str(parent.orderId)
            self.ib.qualifyContracts(contract)
            ##SAVE STATUS FOR EACH OF THE ORDERS SUBMITTED AND SEND ALERT IF WE ALREADY ACQUIRED SHARES#
            for trade in self.ib.trades():
                orderId = str(trade.orderStatus.orderId)
                status = trade.orderStatus.status
                if orderId in self.data[submittedTimeKey]["Orders"].keys():
                    self.data[submittedTimeKey]["OrderStatus"][orderId] = status
                    if self.data[submittedTimeKey]["Orders"][orderId] == "BUY" and status == "Filled":            #This sends alert if it filled immediately which may happen sometimes
                        print("%s Order Completed; AvgFillPrice: %s; Filled: %s"%(self.data[submittedTimeKey]["Orders"][orderId], trade.orderStatus.avgFillPrice, trade.orderStatus.filled))
                        self.data[submittedTimeKey]["OpenPrice"] = trade.orderStatus.avgFillPrice
                        #if self.SEND_ALERTS:
                        #    self.com.sendMsgToQueue("%s %s Order Completed; AvgFillPrice: %s; Filled: %s"%(self.strategyID,self.data[submittedTimeKey]["Orders"][orderId],trade.orderStatus.avgFillPrice,trade.orderStatus.filled),contactList=self.CONTACTGROUP)
                        self.data[submittedTimeKey]["Status"] = "ACQUIRED"
            
        print("Buy Order Submitted for Option %s at %.2f"%(contract.localSymbol,buyDict["ENTRYLIMIT"]))
        #if self.SEND_ALERTS:
        #    self.com.sendMsgToQueue("%s Buy Order Submitted for Option %s at %.2f; Target: %.2f  Stop: %.2f"%(self.strategyID,contract.localSymbol,buyDict["ENTRYLIMIT"],buyDict["TARGETLIMIT"],buyDict["STOPLIMIT"]),contactList=self.CONTACTGROUP)
        self.saveDataToFile()

    def checkPositionStatus(self, contract, df, timestamp, orderSubmissionTime):
        contractSymbol = contract.localSymbol
        self.ib.qualifyContracts(contract)
        submittedTimeKey = orderSubmissionTime
        timestamp = timestamp.replace(second=0,microsecond=0)
        if not isinstance(orderSubmissionTime, str):
            submittedTimeKey = orderSubmissionTime.strftime('%H:%M:%S')
        if self.TRADE_ENABLED:
            Fills = {}
            for trade in self.ib.trades():
                order    = trade.order
                orderStatus = trade.orderStatus
                orderId = str(trade.orderStatus.orderId)
                status = trade.orderStatus.status
                if orderId == "0":
                    try:
                        if trade.order.permId in [682098252]:
                            orderId = str(4363)
                            print(orderId)
                        else:
                            orderId = str(trade.fills[0].execution.orderId)
                    except Exception as detail:
                        continue
                if submittedTimeKey in self.data.keys() and orderId in self.data[submittedTimeKey]["Orders"].keys():
                    #Check if we own the stock and there is a STP adjust flag in the properties and if we have exceeded what is specified. If so, raise the STP LIMIT#
                    if status != self.data[submittedTimeKey]["OrderStatus"][orderId]:
                        #print(trade)
                        if status not in ["Submitted", "PreSubmitted", "Filled", "PendingCancel", "Cancelled"]:
                            print("Order status Changed for %s from %s to %s"%(self.data[submittedTimeKey]["Orders"][orderId], self.data[submittedTimeKey]["OrderStatus"][orderId], status))
                            if self.SEND_ALERTS:
                                self.com.sendMsgToQueue("%s %s Order status Changed for %s from %s to %s"%(self.strategyID,contractSymbol,self.data[submittedTimeKey]["Orders"][orderId],self.data[submittedTimeKey]["OrderStatus"][orderId],status),contactList=self.CONTACTGROUP)
                        self.data[submittedTimeKey]["OrderStatus"][orderId] = status
                        if status == "Filled":
                            fillPrice = trade.orderStatus.avgFillPrice
                            qty       = trade.orderStatus.filled
                            if qty == 0.0 or fillPrice == 0.0:
                                try:
                                    qty = trade.order.filledQuantity
                                    fillPrice = trade.fills[0].execution.avgPrice
                                except Exception as detail:
                                    continue
                            print("%s Order Completed; AvgFillPrice: %s; Filled: %s"%(self.data[submittedTimeKey]["Orders"][orderId], fillPrice, qty))
                            if self.SEND_ALERTS:
                                self.com.sendMsgToQueue("%s %s %s Order Completed; AvgFillPrice: %s; Filled: %s"%(self.strategyID, contractSymbol, self.data[submittedTimeKey]["Orders"][orderId], fillPrice, qty), contactList=self.CONTACTGROUP)
                            if self.data[submittedTimeKey]["Orders"][orderId] == "BUY":
                                self.data[submittedTimeKey]["Status"] = "ACQUIRED"
                                self.data[submittedTimeKey]["LoD"] = df[df['date']==timestamp]['low'].item()  #Initialize the LoD attribute
                                self.data[submittedTimeKey]["OpenPrice"] = fillPrice
                            else:
                                self.data[submittedTimeKey]["Status"] = "SOLD"
                                self.data[submittedTimeKey]["ClosedPrice"] = fillPrice
                        elif status not in ["Cancelled","Inactive","Submitted","PreSubmitted", "PendingCancel"]:  #Could be partially filled#
                            print("%s Order Update; AvgFillPrice: %s; Remaining: %s"%(self.data[submittedTimeKey]["Orders"][orderId], trade.orderStatus.avgFillPrice, trade.orderStatus.remaining))
                            if self.SEND_ALERTS:
                                self.com.sendMsgToQueue("%s %s %s Order Update; AvgFillPrice: %s; Remaining: %s"%(self.strategyID, contractSymbol, self.data[submittedTimeKey]["Orders"][orderId], trade.orderStatus.avgFillPrice, trade.orderStatus.remaining), contactList=self.CONTACTGROUP)
                        elif status  in ["Cancelled","Inactive"]:
                            print("%s order %s; Reason: %s"%(self.data[submittedTimeKey]["Orders"][orderId], status, trade.log[-1].message))
                            if self.data[submittedTimeKey]["Orders"][orderId] == "BUY" or self.data[submittedTimeKey]["BuyOrderId"] in trade.log[-1].message:
                                self.data[submittedTimeKey]["Status"] = "CANCELLED"
                                if self.SEND_ALERTS:
                                    self.com.sendMsgToQueue("%s %s Buy order Cancelled; Reason: %s"%(self.strategyID, contractSymbol, trade.log[-1].message), contactList=self.CONTACTGROUP)
                    else:
                        if self.data[submittedTimeKey]["Orders"][orderId] == "BUY" and trade.filled() == 0:  #We haven't yet filled any of our buy shares
                            expirationTime = datetime.strptime(self.data[submittedTimeKey]["Expiration"],'%Y%m%d %H:%M:%S')
                            if timestamp >= expirationTime:
                                print("Expired")
                                if status not in ["Cancelled","Inactive"]:  #We only want to cancel active orders; otherwise we get exception
                                    self.ib.cancelOrder(trade.order)
                                    self.data[submittedTimeKey]["Status"] = "EXPIRED"     #This will be changed to CANCELLED as soon as broker confirms cancellation status.
                                if self.data[submittedTimeKey]["Status"] == "EXPIRED" and status == "Cancelled":
                                    print("Update Status to cancelled")
                                    self.data[submittedTimeKey]["Status"] = "CANCELLED"
                                    if self.SEND_ALERTS:
                                        self.com.sendMsgToQueue("%s %s Buy order Cancelled; Reason: Expired"%(self.strategyID, contractSymbol), contactList=self.CONTACTGROUP)
                    #RAISE STP LIMIT UP IF WE PAID MORE THAN EXPECTED FOR A MKT ORDER#
                    if self.data[submittedTimeKey]["Status"] == "ACQUIRED" and "OpenPrice" in self.data[submittedTimeKey] and self.data[submittedTimeKey]["Orders"][orderId] == "STP":
                        if self.data[submittedTimeKey]["OpenPrice"] > self.data[submittedTimeKey]["LimitPrice"]:
                            D = decimal.Decimal
                            cent = D('0.01')
                            expected   = self.data[submittedTimeKey]["LimitPrice"]
                            actualFill = self.data[submittedTimeKey]["OpenPrice"]
                            oldStop    = self.data[submittedTimeKey]["StopLimit"]
                            newStop    = actualFill*oldStop/expected
                            newStop = float(D('%.4f'%newStop).quantize(cent,rounding=decimal.ROUND_DOWN))  #round down to nearest cent
                            order.auxPrice = newStop
                            updateTrade = self.ib.placeOrder(contract, order)
                            self.ib.qualifyContracts()
                            self.data[submittedTimeKey]["LimitPrice"] = actualFill
                            self.data[submittedTimeKey]["StopLimit"] = newStop
                            print("Updated LimitPrice and StopLimit in self.data to match actual: %.2f;%.2f"%(actualFill,newStop))
                    #ADJUST STP LIMIT UP IF WE HAVE THE POSITION AND THE HIGH HAS BEEN RAISED#
                    if self.data[submittedTimeKey]["Status"] == "ACQUIRED" and "STOPADJUSTTRIGGER" in self.data[submittedTimeKey]["Props"] and self.data[submittedTimeKey]["Orders"][orderId] == "STP":
                        if df[df['date']==timestamp]['high'].item() > self.data[submittedTimeKey]["LimitPrice"]*(1+self.data[submittedTimeKey]["Props"]["STOPADJUSTTRIGGER"]): #Raise Stop Limit
                            D = decimal.Decimal
                            cent = D('0.01')
                            newStop   = self.data[submittedTimeKey]["LimitPrice"]*(1+self.data[submittedTimeKey]["Props"]["STOPADJUSTTARGET"])              #Raise stop limit to the STOPADJUSTTARGET
                            order.auxPrice = float(D('%.4f'%newStop).quantize(cent,rounding=decimal.ROUND_DOWN))  #round down to nearest cent
                            updateTrade = self.ib.placeOrder(contract, order)
                            self.ib.qualifyContracts()
                    #ADJUST TRGT LIMIT DOWN IF WE HAVE THE POSITION AND WE ARE USING LOSS2PROFITADJUSTED#
                    if self.data[submittedTimeKey]["Status"] == "ACQUIRED" and "LOSS2PROFITADJUSTED" in self.data[submittedTimeKey]["Props"] and self.data[submittedTimeKey]["Orders"][orderId] == "PROFIT":
                        minuteLow = df[df['date']==timestamp]['low'].item()
                        print("minuteLow: %.2f @ %s"%(minuteLow,timestamp))
                        if minuteLow < self.data[submittedTimeKey]["LoD"]:
                            purchasePrice = self.data[submittedTimeKey]["LimitPrice"]
                            LowOfDay = minuteLow
                            self.data[submittedTimeKey]["LoD"] = LowOfDay                     #Set new Low-of-day value and drop the target
                            targetPercent = self.data[submittedTimeKey]["Props"]["TARGET"] + (LowOfDay - purchasePrice)/purchasePrice
                            D = decimal.Decimal
                            cent = D('0.01')
                            newTarget   = self.data[submittedTimeKey]["LimitPrice"]*(1+targetPercent)                 #Raise stop limit to the STOPADJUSTTARGET
                            order.lmtPrice = float(D('%.4f'%newTarget).quantize(cent,rounding=decimal.ROUND_UP))  #round down to nearest cent
                            order.transmit = True
                            updateTrade = self.ib.placeOrder(contract, order)
                            print("Target adjusted to %.2f"%order.lmtPrice)
                            self.ib.qualifyContracts()


        else:
            #print(df[-1:])
            #print(self.data[submittedTimeKey]["Status"])
            if timestamp in list(df['date']):
                if timestamp.strftime('%Y%m%d %H:%M:%S') != self.data[submittedTimeKey]["TriggerTime"]:
                    if self.data[submittedTimeKey]["Status"]=="SUBMITTED":
                        #Check to see if limit order gets filled
                        expirationTime = datetime.strptime(self.data[submittedTimeKey]["Expiration"],'%Y%m%d %H:%M:%S')
                        if df[df['date']==timestamp]['low'].item() <= self.data[submittedTimeKey]["LimitPrice"]:
                            self.data[submittedTimeKey]["Status"] = "ACQUIRED"
                            print("Order Filled for Option %s at %.2f"%(contractSymbol,self.data[submittedTimeKey]["LimitPrice"]))
                            if self.SEND_ALERTS:
                                self.com.sendMsgToQueue("%s Order Filled for Stock %s at %.2f"%(self.strategyID, contractSymbol, self.data[submittedTimeKey]["LimitPrice"]), contactList=self.CONTACTGROUP)
                        elif timestamp >= expirationTime:
                            self.data[submittedTimeKey]["Status"] = "CANCELLED"
                            if self.SEND_ALERTS:
                                self.com.sendMsgToQueue("%s Order Expired for Stock %s"%(self.strategyID, contractSymbol), contactList=self.CONTACTGROUP)
                    elif self.data[submittedTimeKey]["Status"]=="ACQUIRED":
                        #Check for Stop Loss or Target Hit
                        if df[df['date']==timestamp]['low'].item() < self.data[submittedTimeKey]["StopLimit"]:
                            self.data[submittedTimeKey]["Status"] = "SOLD"
                            self.data[submittedTimeKey]["ClosedPrice"] = self.data[submittedTimeKey]["StopLimit"]
                            print("Stop Limit Order Filled for Option %s at %.2f"%(contractSymbol,self.data[submittedTimeKey]["ClosedPrice"]))
                            if self.SEND_ALERTS:
                                self.com.sendMsgToQueue("%s Stop Limit Order Filled for Stock %s at %.2f"%(self.strategyID, contractSymbol, self.data[submittedTimeKey]["StopLimit"]), contactList=self.CONTACTGROUP)
                        elif df[df['date']==timestamp]['high'].item() > self.data[submittedTimeKey]["TargetLimit"]:
                            self.data[submittedTimeKey]["Status"] = "SOLD"
                            self.data[submittedTimeKey]["ClosedPrice"] = self.data[submittedTimeKey]["TargetLimit"]
                            print("Target Limit Order Filled for Option %s at %.2f"%(contractSymbol,self.data[submittedTimeKey]["ClosedPrice"]))
                            if self.SEND_ALERTS:
                                self.com.sendMsgToQueue("%s Target Limit Order Filled for Stock %s at %.2f"%(self.strategyID, contractSymbol, self.data[submittedTimeKey]["TargetLimit"]), contactList=self.CONTACTGROUP)
            else:
                print("Timestamp %s not in df"%timestamp)
                #print(list(df['date']))

        self.saveDataToFile()
        return(self.data[submittedTimeKey]["Status"])

    def closeOpenPositions(self, minuteDataDict, timestamp, optionSymbol=None):  #Run this at the market Closing Bell#
        timestamp = timestamp.replace(second=0,microsecond=0)
        if self.TRADE_ENABLED: ##We need to exit our position
            #Loop Through Submitted Trades and cancel open buys and exit open positions#
            activeOptionsSymbols = []
            if optionSymbol is not None:
                activeOptionsSymbols = [optionSymbol]
            else:
                for submittedTimeKey,tradeDict in self.data.items():
                    if tradeDict["localSymbol"] not in activeOptionsSymbols:
                        activeOptionsSymbols.append(tradeDict["localSymbol"])
            for trade in self.ib.trades():
                contract = trade.contract
                order    = trade.order
                orderStatus = trade.orderStatus
                orderId = str(trade.orderStatus.orderId)
                orderSubmissionTime = self.orderIdTable.get(orderId)
                if contract.localSymbol in activeOptionsSymbols:
                    if order.action=='BUY' and orderStatus.status in ["Submitted","PendingSubmit","PreSubmitted"]: #Cancel Open BUY Orders#
                        closeTrade = self.ib.cancelOrder(trade.order)
                    if order.action == 'SELL' and orderStatus.status == "Submitted":  #Change Profit order to market order# 
                        order.orderType='MKT'
                        order.transmit=True
                        closeTrade = self.ib.placeOrder(contract, order)
                    self.ib.qualifyContracts()
                    time.sleep(1)
                    if orderSubmissionTime:
                        self.checkPositionStatus(contract, minuteDataDict[contract.localSymbol], timestamp, orderSubmissionTime)
                    else:
                        print("Order closed, but we didn't check the status because the orderId was not in our status table.")

        for submittedTimeKey,tradeDict in self.data.items():
            if not self.TRADE_ENABLED and self.SEND_ALERTS:
                openOrders = ["SUBMITTED","ACQUIRED","EXPIRED"]
                if optionSymbol==None or tradeDict["localSymbol"] == optionSymbol:
                    if tradeDict["Status"] in ["SUBMITTED","EXPIRED"]:
                        tradeDict["Status"] = "CANCELLED"
                    if tradeDict["Status"] in ["ACQUIRED"]:
                        df = minuteDataDict[tradeDict["localSymbol"]]
                        tradeDict["ClosedPrice"] = df[df['date']==timestamp]['close'].item()  #Assume sell price at the last minute close#
                        tradeDict["Status"] = "SOLD"
                        print("END OF DAY Sell for Option %s at %.2f"%(tradeDict["localSymbol"],tradeDict["ClosedPrice"]))
                        self.com.sendMsgToQueue("END OF DAY Sell for Option %s at %.2f"%(tradeDict["localSymbol"],tradeDict["ClosedPrice"]), contactList=self.CONTACTGROUP)

    def endOfDayPositionChecker(self):
        if self.TRADE_ENABLED:
            Message = "\n%s: EODValidation"%self.strategyID
            for submittedTimeKey,tradeDict in self.data.items():
                if tradeDict["Status"] in ["SOLD","CANCELLED","EXPIRED"]:
                    tradeDict["EODVerified"] = True
                    Message = Message + "\n%s: Pass"%tradeDict["localSymbol"]
                else:
                    tradeDict["EODVerified"] = False
                    Message = Message + "\n%s: Fail!"%tradeDict["localSymbol"]
            if len(self.data.items()) > 0 and self.EODCheckComplete == False:
                if self.SEND_ALERTS:
                    self.com.sendMsgToQueue(Message, contactList=self.CONTACTGROUP)
                self.EODCheckComplete = True

        if len(self.data.keys())==0:    #If Dictionary is empty, let's erase the file
            if os.path.exists(self.ActiveOrderFile):
                os.remove(self.ActiveOrderFile)
