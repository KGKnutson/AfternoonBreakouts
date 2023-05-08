import json
import os
import unittest

class StockProps(object):
    data = None
    PUTCALLIDX = 12

    def __init__(self):
        #Read .xls content into program memory#
        #Definitions = os.getenv('INPUTTARGETSTOP')
        with open("StockProps.json", 'r') as f:
            self.data = json.load(f)


    def getTriggerProperties(self, strategy, TriggerIndex):
        DEFAULT_KEY = "DEFAULT"
        triggerPropertiesDict = self.data[strategy.upper()]["TRIGGER_MINUTE"][DEFAULT_KEY].copy()  #Make a copy of the dictionary so we don't change the originals
        
        #Overwrite any default values if their is an override applicable for the current minute (aka index)
        OVERRIDE_KEY = None
        TriggerPeriods = list(self.data[strategy.upper()]["TRIGGER_MINUTE"].keys())
        TriggerPeriods.remove(DEFAULT_KEY)  #Remove Default Key from the list so we can loop through the integers and see if there is a match.
        TriggerPeriods = [eval(i) for i in TriggerPeriods]  #Convert trigger list of strings to list of ints
        for index in sorted(TriggerPeriods):       
            if int(TriggerIndex) <= int(index):
                #Override the original key,value pairs using those found in the indexed dictionary
                OVERRIDE_KEY = str(index)         #Found a trigger window match to use besides the default
                for key,value in self.data[underlying.upper()]["TRIGGER_MINUTE"][OVERRIDE_KEY].items():
                    triggerPropertiesDict[key] = value
                break
        #Return the resulting dictionary
        return(triggerPropertiesDict)



class TestStockProps(unittest.TestCase):
    op = StockProps()

    def setUp(self):
        print("%s"%self._testMethodName)

    def tearDown(self):
        self.assertEqual(self.response["TARGET"], self.ExpectedTarget)
        self.assertEqual(self.response["STOP"], self.ExpectedStop)
        self.assertEqual(self.response["MAXDAILYBUYS"], self.ExpectedBuys)
        self.assertEqual(self.response["TRIGGER_DISCOUNT"], self.ExpectedDiscount)
        self.assertEqual(self.response["EXPIRATION"], self.ExpectedExpiration)
        self.assertEqual(self.response["ORDER_TYPE"], self.ExpectedOrderType)

    def test_AfternoonBreakouts_default(self):
        strategy = "AfternoonBreakouts"
        trigger = 60
        self.ExpectedTarget = 2.0
        self.ExpectedStop = -0.12
        self.ExpectedBuys = 5
        self.ExpectedDiscount = 0
        self.ExpectedExpiration = 60
        self.ExpectedOrderType = "LMT"
        self.response = self.op.getTriggerProperties(strategy, trigger)

if __name__ == '__main__':
    unittest.main()