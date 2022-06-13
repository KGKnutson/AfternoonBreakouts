import os
import pandas as pd
import unittest
import numpy as np

'''
This class returns the "target" and "stop" for a particular setup based on stock rank as input.
'''
class GetTargetStop(object):
    df = None

    def __init__(self):
        #Read .xls content into program memory#
        #Definitions = os.getenv('INPUTTARGETSTOP')
        targetDefs = "InputTargetStop.xls"
        if os.path.exists(targetDefs):
            self.df = pd.read_excel(targetDefs)
            self.df = self.df.replace(np.nan,'N/A')
            #print(self.df)

    def getTargetStop(self, rank):
        #Excel stores times as fractions of a day. You can convert this to a Python time as follows:
        for index,row in self.df.iterrows():
            if row['Rank']:
                if rank != row['Rank']:
                    continue
                return row['Target'],row['Stop'],row['HolidayTarget'],row['HolidayStop'],row['MarketAverageMin']


class TestGetTargetStop(unittest.TestCase):
    gts = GetTargetStop()

    def setUp(self):
        print("%s"%self._testMethodName)

    def tearDown(self):
        self.assertEqual(self.response[0], self.ExpectedTarget)
        self.assertEqual(self.response[1], self.ExpectedStop)
        self.assertEqual(self.response[2], self.ExpectedHolidayTarget)
        self.assertEqual(self.response[3], self.ExpectedHolidayStop)
        self.assertEqual(self.response[4], self.ExpectedMarketMin)

    def test_Rank1(self):
        Rank = 1
        self.ExpectedTarget = "EOD"
        self.ExpectedStop = -.1
        self.ExpectedHolidayTarget = .06
        self.ExpectedHolidayStop = -.05
        self.ExpectedMarketMin = "N/A"
        self.response = self.gts.getTargetStop(Rank)

    def test_Rank2(self):
        Rank = 2
        self.ExpectedTarget = "EOD"
        self.ExpectedStop = -.05
        self.ExpectedHolidayTarget = .06
        self.ExpectedHolidayStop = -.05
        self.ExpectedMarketMin = "N/A"
        self.response = self.gts.getTargetStop(Rank)

    def test_Rank3(self):
        Rank = 3
        self.ExpectedTarget = .06
        self.ExpectedStop = -.05
        self.ExpectedHolidayTarget = .06
        self.ExpectedHolidayStop = -.05
        self.ExpectedMarketMin = "N/A"
        self.response = self.gts.getTargetStop(Rank)

    def test_Rank4(self):
        Rank = 4
        self.ExpectedTarget = "N/A"
        self.ExpectedStop = "N/A"
        self.ExpectedHolidayTarget = "N/A"
        self.ExpectedHolidayStop = "N/A"
        self.ExpectedMarketMin = "N/A"
        self.response = self.gts.getTargetStop(Rank)

if __name__ == '__main__':
    unittest.main()