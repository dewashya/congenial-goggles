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
        #print(self.end)
        self.url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?&frmdt={self.start}&todt=10-Apr-2006" #{self.end}"

    async def get_corrupted_data(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                a = await response.text()
                b = await response.text()    
        return print(a[:50],"Baka\n", b[:50])
    
asyncio.run(Err().get_corrupted_data())

