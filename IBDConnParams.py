import json
import os
import unittest

'''
    DEFINITIONS in IBDConnParams.json:
    "clientId": {
        "DEVELOPMENT": 1,
        "OPTIONTRADER": 2,
        "TWSALIVECHECK": 3,
        "IBDINTEGRITY": 4,
        "OPTIONBACKUPQQQ": 5,
        "OPTIONBACKUPSPY": 6,
        "OPTIONBACKUPDIA": 7,
        "EVOLVEDTRADER": 8,
        "TWSRESTARTCHECK": 9
    },
    "connPort": {
        "PAPER": 7497,
        "CASH": 7496
    },
    "RPCHost": "wakeboard",
    "heartbeatCheckers": [
        "heartbeat_wakeboard",
        "twsChecker_wakeboard",
        "winlocked_wakeboard",
        "strategy1_wakeboard",
        "msgProcessing_wakeboard",
        "heartbeat_leetrades2",
        "twsChecker_leetrades2",
        "winlocked_leetrades2",
        "strategy1_leetrades2"
    ],
    "maxDelays": {
        "heartbeat":    120,
        "strategy1":    60,
        "winlocked":    3700,
        "msgProcessing":120,
        "twsChecker":   3700
        },
    "operationHours": {
        "winlocked": {"B4MRKT":9.5,"AFTRMRKT": 0.0},
        "strategy1": {"B4MRKT":0.0,"AFTRMRKT": 0.0},
        "twsChecker": {"B4MRKT":3.0,"AFTRMRKT": 1.0},
        "msgProcessing": {"B4MRKT":9.5,"AFTRMRKT": 1.0},
        "heartbeat": {"B4MRKT":9.5,"AFTRMRKT": 1.0}
        }
'''
class IBDConnParams(object):
    data = None
    jsonFile = "IBDConnParams.json"

    def __init__(self):
        #Read .xls content into program memory#
        #Definitions = os.getenv('INPUTTARGETSTOP')
        with open(self.jsonFile, 'r') as f:
            self.data = json.load(f)
    
    def getclientID(self, LookupKey):
        clientId = self.data["clientId"][LookupKey]
        return(clientId)

    def getConnPort(self, accountType):
        connPort = self.data["connPort"][accountType]
        return(connPort)

    def getRPCHost(self):
        rpcHost = self.data["RPCHost"]
        return(rpcHost)

    def getMaxDelays(self):
        maxDelays = self.data["maxDelays"]
        return(maxDelays)

    def getHeartbeatCheckers(self):
        checkers = self.data["heartbeatCheckers"]
        return(checkers)

    def getInvestmentSize(self, machineID, strategy):
        key = "%s_%s"%(strategy.lower(),machineID.lower())
        investmentSize = self.data["investmentSize"][key]
        return(investmentSize)

    def getOperationalHours(self):
        hours = self.data["operationHours"]
        return(hours)

class TestIBDConnParams(unittest.TestCase):
    connParam = IBDConnParams()

    def setUp(self):
        print("%s"%self._testMethodName)

    def tearDown(self):
        self.assertEqual(self.response, self.ExpectedResponse)

    def test_DEVELOPMENT(self):
        self.ExpectedResponse = 1
        self.response = self.connParam.getclientID("DEVELOPMENT")

    def test_OPTIONTRADER(self):
        self.ExpectedResponse = 2
        self.response = self.connParam.getclientID("OPTIONTRADER")
        
    def test_IBDIntegrity(self):
        self.ExpectedResponse = 4
        self.response = self.connParam.getclientID("IBDINTEGRITY")

    def test_BackupQQQ(self):
        self.ExpectedResponse = 5
        self.response = self.connParam.getclientID("OPTIONBACKUPQQQ")

    def test_BackupSPY(self):
        self.ExpectedResponse = 6
        self.response = self.connParam.getclientID("OPTIONBACKUPSPY")

    def test_BackupDIA(self):
        self.ExpectedResponse = 7
        self.response = self.connParam.getclientID("OPTIONBACKUPDIA")

    def test_EvolvedTrader(self):
        self.ExpectedResponse = 8
        self.response = self.connParam.getclientID("EVOLVEDTRADER")

    def test_ConnPortCash(self):
        self.ExpectedResponse = 7496
        self.response = self.connParam.getConnPort("CASH")

    def test_ConnPortPaper(self):
        self.ExpectedResponse = 7497
        self.response = self.connParam.getConnPort("PAPER")

    def test_RPCHost(self):
        self.ExpectedResponse = "lenovothinkpad"
        self.response = self.connParam.getRPCHost()

    def test_HBCheckers(self):
        self.ExpectedResponse = ["heartbeat_lenovothinkpad","heartbeat_toshiba","strategy1_lenovothinkpad"]
        self.response = self.connParam.getHeartbeatCheckers()
     

if __name__ == '__main__':
    unittest.main()