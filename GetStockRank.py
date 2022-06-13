import os
import pandas as pd
import unittest

'''
This class returns the "rank" for a stock based on float and market cap according to an input definition file defined by INPUTRANK environment variable
'''
class GetStockRank(object):
    df = None

    def __init__(self):
        #Read .xls content into program memory#
        #RankDefinitions = os.getenv('INPUTRANK')
        rankdefinitions = "InputRank.xls"
        if os.path.exists(rankdefinitions):
            self.df = pd.read_excel(rankdefinitions)
            #print(self.df)

    def getStockRank(self, float, marketCap):
        #Excel stores times as fractions of a day. You can convert this to a Python time as follows:
        for index,row in self.df.iterrows():
            if row['FloatMax']:
                if float > row['FloatMax']:
                    continue
            if row['FloatMin']:
                if float <= row['FloatMin']:
                    continue
            if row['MarketCapMax']:
                if marketCap > row['MarketCapMax']:
                    continue
            if row['MarketCapMin']:
                if marketCap <= row['MarketCapMin']:
                    continue
            return row['Rank']


class TestGetStockRank(unittest.TestCase):
    gsr = GetStockRank()

    def setUp(self):
        print("%s"%self._testMethodName)

    def test_Rank1(self):
        MarketCap = 100000000
        Float = 30000000
        ExpectedRank = 1
        self.assertEqual(self.gsr.getStockRank(Float,MarketCap), ExpectedRank)

    def test_Rank2Market(self):
        MarketCap = 100000001
        Float = 30000000
        ExpectedRank = 2
        self.assertEqual(self.gsr.getStockRank(Float,MarketCap), ExpectedRank)

    def test_Rank2Float(self):
        MarketCap = 100000000
        Float = 30000001
        ExpectedRank = 2
        self.assertEqual(self.gsr.getStockRank(Float,MarketCap), ExpectedRank)


    def test_Rank3(self):
        MarketCap = 100000001
        Float = 30000001
        ExpectedRank = 3
        self.assertEqual(self.gsr.getStockRank(Float,MarketCap), ExpectedRank)


    def test_Rank4(self):
        MarketCap = 1000000001  #$1B
        Float = 30000000
        ExpectedRank = 4
        self.assertEqual(self.gsr.getStockRank(Float,MarketCap), ExpectedRank)

if __name__ == '__main__':
    unittest.main()