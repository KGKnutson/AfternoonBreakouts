# AfternoonBreakouts

Repository to store my Afternoon Breakouts Tracking Code; 
1. ProcessTopPercentGainers.py - This currently works and is what is used to identify potential breakouts to trade during the market hours.  It is continually looking for stocks breaking out during the market hours.
                                 This data gets saved and can be studied later for potential trade opportunities in the future.
2. The AfternoonBreakouts.py works and it contains support code for ProcessTopPercentGainers tracking stocks that are breaking out.
3. The UpdateAfternoonBreakouts.py - This is run after the market close to analyze the potential performance of the breakouts that were found that day.
                                     This needs some updating as one of my datasources stopped working. A replacement needs to be found.
4. StockProps.json contains the rules that could be used to trade off of breakouts that are found during the market day.
5. StockTrader.py needs further development but is the program that would do the trading during the market day if we wanted to trade certain breakouts that are found.
6. BackTestBreakouts.py needs some further development but would show the results of each day's stocks that were found from the ProcessTopPercentGainers.py program.