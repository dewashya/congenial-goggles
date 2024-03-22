"""
In this Script I try to extract Error NAV value and Scheme Code
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
                print(adf.columns, adf.head(10))
                print(bdf.columns, bdf.head(10))
        return 
asyncio.run(Err().get_corrupted_data())

