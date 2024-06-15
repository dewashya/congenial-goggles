"""
In this Script I try to extract Error NAV value and Scheme Code

The main problem is in NAV there are some error value which is not the ideal float value. i want to figure out those rows and 
keep them to refer again while cleaning the data. 
"""

import asyncio
import datetime
import aiohttp
import pandas
class Err:
    def __init__(self):
        self.start = "03-Apr-2006"
        self.end = datetime.date.today().strftime("%d-%b-%Y")
        #print(self.end)
        self.url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?&frmdt={self.start}&todt=10-Apr-2006" #{self.end}" 

    async def get_corrupted_data(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                a = await response.text()
                b = await response.text()
                a0data = a.split("\r\n")
                b0data = b.split("\r\n")
                adf = pandas.DataFrame([i.split(";") for i in a0data[1:]],columns=a0data[0].split(";") )
                bdf = pandas.DataFrame([j.split(";") for j in b0data[1:]],columns=b0data[0].split(";") )
                adf.infer_objects()
                bdf.infer_objects()
                print(adf.head())
                print(list(adf["Net Asset Value"]))
                print(list(bdf["Net Asset Value"]))
        return 
asyncio.run(Err().get_corrupted_data())

