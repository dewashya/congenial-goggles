"""
In this Script I try to extract Error NAV value and Scheme Code

The main problem is in NAV there are some error value which is not the ideal float value. i want to figure out those rows and 
keep them to refer again while cleaning the data. 
"""

import asyncio
import datetime
import aiohttp
import pandas
import mysql.connector


class Err:
    def __init__(self):
        self.start = "03-Apr-2006"
        self.end = (datetime.date.today()-datetime.timedelta(days=1)).strftime("%d-%b-%Y")
        self.url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?&frmdt={self.start}&todt={self.end}"
        print(self.url)
        self.cnx = mysql.connector.connect(user='root', password='De$#Ka@)*01Zz')
        if self.cnx.is_connected() == True:
            print("Connection made!")

    def split_date(self, start_date, end_date, ran):
        start_date= datetime.strptime(start_date, "%d-%b-%Y")
        end_date = datetime.strptime(end_date, "%d-%b-%Y")

        date_ranges = []
        range_start = start_date

        while start_date < end_date:
            range_end = range_start + datetime.timedelta(days=ran - 1)

            if range_end > end_date:
                range_end = end_date

            date_ranges.append((range_start, range_end))
            range_start = range_end + datetime.timedelta(days=1)
        
        return date_ranges

    def check_db(self):
        self.cnx.close()
        

    async def get_corrupted_data(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                a = await response.text()
        return a
    
    # def get_data():
        
    
aa = Err()
aa.check_db()
# asyncio.run(Err().get_corrupted_data())

