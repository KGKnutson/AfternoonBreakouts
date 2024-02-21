import smtplib
import ssl
import os
import sys
import traceback
import logging
import zerorpc
import socket
import six
import pandas_market_calendars as mcal
from IBDConnParams import *
from HostTracker import *
from datetime import *
from twilio.rest import Client
import time

class Communicator(object):
    try:
        SLEEP_DELAY = 60
        logFile = os.path.join(os.environ.get("OPTIONTRADERLOGPATH"),"%s_%s.txt"%("Communicator",datetime.now().strftime('%Y%m%d')))
        logging.basicConfig(filename=logFile, filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        dContacts = {}
        dContacts["Machine1"] =       {
                                        "Joe":"Joe@tmomail.net",
                                        "Vivian":"Vivian@tmomail.net",
                                      }
        dContacts["Machine2"] =       {
                                        "Stella":"Stella@tmomail.net",
                                        "Gabe":"Gabe@tmomail.net",
                                      }
        dContacts["LEETRADES"] =      {
                                        "KevinEmail":"retireconfire@gmail.com",
                                      }
        dContacts["LENOVOTHINKPAD"] = {
                                        "KevinEmail":"retireconfire@gmail.com",
                                      }
        dContacts["LENOVO"] =         {
                                        "KevinEmail":"retireconfire@gmail.com",
                                      }
        dContacts["LEETRADES2"] =     {
                                        "KevinEmail":"retireconfire@gmail.com",
                                      }
        dContacts["WAKEBOARD"] =      {
                                        "KevinEmail":"retireconfire@gmail.com",
                                      }
                              
        dContacts["ALERT"] =          {
                                        "KevinEmail":"retireconfire@gmail.com",
                                      }
        dContacts["REBOOT_ALERT"] =   {
                                        "KevinEmail":"retireconfire@gmail.com",
                                      }
        dContacts["SUPPORT"] =        {
                                        "KevinEmail":"retireconfire@gmail.com",
                                      }
        connParam = IBDConnParams()
        hostTracker = HostTracker(connParam)
        rpcHost = None
        operationalHours = connParam.getOperationalHours()
        nyse = mcal.get_calendar('NYSE')
        today = datetime.now().strftime('%Y-%m-%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        today_close = today_open = tomorrow_close = tomorrow_open = None
        try:
            marketHoursToday = nyse.schedule(today,today)
            marketHoursTomorrow = nyse.schedule(tomorrow,tomorrow)
            if not marketHoursToday.empty:
                today_close = datetime.fromtimestamp(marketHoursToday.market_close.item().timestamp())
                today_open = datetime.fromtimestamp(marketHoursToday.market_open.item().timestamp())
            if not marketHoursTomorrow.empty:
                tomorrow_close = datetime.fromtimestamp(marketHoursTomorrow.market_close.item().timestamp())
                tomorrow_open = datetime.fromtimestamp(marketHoursTomorrow.market_open.item().timestamp())
        except Exception as detail:
            logging.critical(traceback.format_exc())
            logging.critical(detail)
            print(detail)
    except Exception as detail:
        logging.critical(traceback.format_exc())
        logging.critical(detail)
        print(detail)

    def ActiveWindow(self, routine):
        if routine in self.operationalHours:
            b4MrktStart = self.operationalHours[routine]["B4MRKT"]
            afterMrktEnd = self.operationalHours[routine]["AFTRMRKT"]
            currentTime = datetime.now()
            b4MrktOffset = -timedelta(hours=abs(b4MrktStart))
            if b4MrktStart < 0:
                b4MrktOffset = timedelta(hours=abs(b4MrktStart))
            if not self.marketHoursToday.empty:
                if currentTime >= (self.today_open + b4MrktOffset) and currentTime <= (self.today_close + timedelta(hours=afterMrktEnd)):
                    #print("Valid for today's range")
                    return True  #Valid for today's range
            if not self.marketHoursTomorrow.empty:
                if currentTime >= (self.tomorrow_open + b4MrktOffset) and currentTime <= (self.tomorrow_close + timedelta(hours=afterMrktEnd)):
                    #print("Valid for tomorrow's range")
                    return True  #Valid for tomorrow's range
            return False
        else:
            return None

    def sleep(self):
        now = datetime.now()
        multiplier=1
        if now.second>=self.SLEEP_DELAY:
            multiplier=2
        time_to_sleep = abs(now.second-self.SLEEP_DELAY*multiplier)
        if time_to_sleep > 0:
            print("Sleeping %d seconds"%time_to_sleep)
            time.sleep(time_to_sleep)
        else:
            print("Didn't sleep")
        return self.SLEEP_DELAY

    def sendMsgToQueue(self, message, contactList="SUPPORT"):
        try:
            if self.rpcHost is None:
                self.rpcConnection()
            if contactList=="EMERGENCY_HEAT":
                print("Sending directly to server for Emergency Heat")
                self.sendAlertMsg(message, email=None, contactList=contactList)
            elif self.rpcHost is not None:
                print("sending message to rpc queue")
                self.rpcHost.sendMessage(message, contactList)
            else:
                print("Sending directly to server; RPCHost Down")
                self.sendAlertMsg(message, email=None, contactList=contactList)
        except Exception as detail:
            logging.critical(traceback.format_exc())
            logging.critical(detail)
            print(detail)
            print("Sending message directly to server")
            self.rpcHost = None
            self.sendAlertMsg(message, email=None, contactList=contactList)
                             #body='Your appointment is coming up on July 21 at 3PM',  
        #print(message, contactList)

    def rpcConnection(self):
        try:
            self.rpcHost = zerorpc.Client()
            self.rpcHost.connect("tcp://%s:4242"%self.hostTracker.getDataHostIPAddress())
        except Exception as detail:
            logging.critical(traceback.format_exc())
            logging.critical(detail)
            print(detail)
            self.rpcHost = None

    def processMsgQueue(self):
        process_key = "msgProcessing"
        while self.ActiveWindow(process_key):
            try:
                MESSAGE = 0
                ALERTGROUP = 1
                processed = []
                if self.rpcHost is None:
                    self.rpcConnection()
                messages = self.rpcHost.getMessages(processed)
                while len(messages)>0:
                    for key,details in six.iteritems(messages):
                        print(key, details)
                        self.sendAlertMsg(details[MESSAGE], email=None, contactList=details[ALERTGROUP])
                        processed.append(key)
                        time.sleep(5)
                    messages = self.rpcHost.getMessages(processed)
                self.rpcHost.notifyIMAlive("%s_%s"%(process_key,socket.gethostname().lower()))      #Notify Server that this machine and program is alive
            except Exception as detail:
                logging.critical(traceback.format_exc())
                logging.critical(detail)
                print(detail)
                self.rpcHost = None

            self.sleep()
        if self.rpcHost is not None:
            self.rpcHost.notifyIMAlive("%s_%s"%(process_key,socket.gethostname().lower()))      #Notify Server that this machine and program is alive
    '''
    Send message directly to server; do not use RPC Data Server
    sMessage = message contents to send to distribution list text.
    email = direct email address to send to or None for a distribution list
    contactList = send to a predefined contactlist
    '''
    def sendAlertMsg(self, sMessage, email=None, contactList="SUPPORT"):
        try:
            username = os.environ.get("EMAIL")
            password = os.environ.get("PWD")
            context=ssl.create_default_context()
            server = None
            try:
                server = smtplib.SMTP('smtp-mail.outlook.com',587)
                server.starttls(context=context)
                server.login(username,password)
            except Exception as detail:
                logging.critical(traceback.format_exc())
                logging.critical(detail)
                print(detail)
                print("Exception thrown trying to connect to outlook.com; Trying once more!")
                time.sleep(3)
                server = smtplib.SMTP('smtp-mail.outlook.com',587)
                server.starttls(context=context)
                server.login(username,password)
            if email:
                self.sendMessage(username, email, sMessage, server)
            elif contactList and contactList in self.dContacts:
                sMessage = "%s: %s"%(contactList,sMessage)
                RecipientList = []
                for contact,email in six.iteritems(self.dContacts[contactList]):
                    RecipientList.append(email)
                self.sendMessage(username, RecipientList, sMessage, server)
            else:
                sMessage = "%s: %s"%("SUPPORT",sMessage)
                RecipientList = []
                for contact,email in six.iteritems(self.dContacts["SUPPORT"]):
                    RecipientList.append(email)
                self.sendMessage(username, RecipientList, sMessage, server)
            server.quit()
            try:
                #Try to send a Twilio Message#
                if contactList and contactList=="EMERGENCY_HEAT":
                    self.sendTwilioMessage(sMessage,"+15096681361")
                else:
                    self.sendTwilioMessage(sMessage,"+15096305967")
            except Exception as detail:
                logging.critical(traceback.format_exc())
                logging.critical(detail)
                print(detail)
        except Exception as detail:
            logging.critical(traceback.format_exc())
            logging.critical(detail)
            print(detail)
            print(traceback.format_exc())
            print("Failed to send alert, we need a backup!")
            print("Failed Message: %s"%sMessage)

    def sendMessage(self, username, email, sMessage, server):
        msg = 'Subject: {}\n\n{}'.format("Stock Alert",sMessage)
        server.sendmail(username, email, msg)

    def sendTwilioMessage(self, sMessage, number):
        # Find your Account SID and Auth Token at twilio.com/console
        # and set the environment variables. See http://twil.io/secure
        account_sid = os.environ['TWILIO_ACCOUNT_SID']
        auth_token = os.environ['TWILIO_AUTH_TOKEN']
        client = Client(account_sid, auth_token)
        
        message = client.messages \
                        .create(
                            body=sMessage,
                            from_='+14323025270',
                            to=number
                        )

        print(message.sid)

if __name__ == "__main__":
    import argparse
    com = Communicator()
    parser = argparse.ArgumentParser()
    parser.add_argument("-email", "--email", help="Send email text message alert.")
    parser.add_argument("-twilio", "--twilio", help="Send a message to Twilio")
    args = parser.parse_args()
    if args.twilio:
        com.sendTwilioMessage("Join Earth's mightiest heroes. Like Kevin Bacon.", "+15096305967")
        #com.sendTwilioMessage("Join Earth's mightiest heroes. Like Kevin Bacon.", "+15096681361")
    elif args.email:
        com.sendAlertMsg(args.email)
    else:       
        com.processMsgQueue()