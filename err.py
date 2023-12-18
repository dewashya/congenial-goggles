"""
In this Script I try to extract Error NAV value and Scheme Code
"""

import asyncio
import datetime
import aiohttp
class Err:
    def __init__(self):
        self.start = "03-Apr-2006"
        self.end = datetime.date.today().strftime("%d-%b-%Y")
        print(self.end)
        self.url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?&frmdt={self.start}&todt=10-Apr-2006" #{self.end}"

    async def get_corrupted_data(self):
        a = await aiohttp.ClientSession().get(self.url)
        b = await aiohttp.ClientSession().get(self.url)
        return a,b
    
asyncio.run(Err().get_corrupted_data())

