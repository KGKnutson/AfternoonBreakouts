import socket

class HostTracker(object):
    def __init__(self, connParam, strategy="strategy1"):
        self.connParam = connParam
        self.RPCHostDict = self.connParam.getRPCHost()
        self.hostName = None
        self.hostIP   = None
        self.strategy = strategy
         
    def getDataHostIPAddress(self):
        return self.RPCHostDict[self.strategy.lower()]["RPC_SERVER_IP"]

    def getHostingProgram(self):
        return self.RPCHostDict[self.strategy.lower()]["IB_DATA_HOST_PROGRAM"]

    def getDataHostName(self):
        STRATEGY = 0
        NAME = 1
        return self.getHostingProgram().split("_")[NAME]

    def IAMIBHostProgram(self):
        myName = socket.gethostname()
        This_Machine_Program_Key = "%s_%s"%(self.strategy.lower(),myName.lower())
        return self.getHostingProgram() == This_Machine_Program_Key
        
        