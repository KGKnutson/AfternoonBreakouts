import os
from datetime import datetime,time
import xlrd
from xlrd import open_workbook

'''
This class returns the closing time for a given day or "CLOSED" if the market is closed
'''
class GetHolidays(object):
    HolidayDict = {}

    def __init__(self, operation):
        #Read .xls content into program memory#
        DATE_COLUMN = 0
        HOLIDAY_COLUMN = 1
        STATUS_COLUMN = 2
        #MarketHolidays = os.getenv('INPUTHOLIDAYS')
        MarketHolidays = "InputHolidays.xls"
        if os.path.exists(MarketHolidays):
            rb = open_workbook(MarketHolidays)
            rs = rb.sheet_by_name("Sheet1")
            for row_idx in range(1, rs.nrows):
                HolidayDate = int(rs.cell(row_idx,DATE_COLUMN).value)
                dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + HolidayDate - 2)
                tt = dt.timetuple()
                HolidayDate = dt.strftime("%Y-%m-%d")
                Holiday = rs.cell(row_idx,HOLIDAY_COLUMN).value
                HolidayStatus = rs.cell(row_idx,STATUS_COLUMN).value
                if str(HolidayStatus).upper() != "CLOSED":
                    HolidayStatus = "%02d:%02d:%02d"%(self.floatHourToTime(HolidayStatus % 1))
                self.HolidayDict[HolidayDate] = HolidayStatus
        else:
            raise Exception("Missing Holiday .xls file from this location: %s"%MarketHolidays)

    def floatHourToTime(self, fh):
        #Excel stores times as fractions of a day. You can convert this to a Python time as follows:
        seconds = int(fh * 24 * 3600) # convert to number of seconds in the day
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return (
            int(hours),
            int(minutes),
            int(seconds),
        )

    def getMarketCloseForDate(self,Date):
        if Date in self.HolidayDict:
            return self.HolidayDict[Date]
        else:
            return "16:00:00"