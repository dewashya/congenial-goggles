"""
In this Script I try to extract Error NAV value and Scheme Code

The main problem is in NAV there are some error value which is not the ideal float value. i want to figure out those rows and 
keep them to refer again while cleaning the data. 
"""

import asyncio
from datetime import datetime, timedelta
import aiohttp
import pandas
import mysql.connector
import requests


class Err:
    def __init__(self):
        self.cnx = mysql.connector.connect(user='root', password='De$#Ka@)*01Zz')
        if self.cnx.is_connected() == True:
            print("Connection made!")
        self.links = self.linkgen()
        

    def split_date(self, start_date, end_date, ran=90)-> list:
        start_date= datetime.strptime(start_date, "%d-%b-%Y")
        end_date = datetime.strptime(end_date, "%d-%b-%Y")
        date_ranges = []
        range_start = start_date
        while range_start < end_date:
            range_end = range_start + timedelta(days=ran - 1)

            if range_end > end_date:
                range_end = end_date

            date_ranges.append((range_start, range_end))
            range_start = range_end + timedelta(days=1)
        
        return date_ranges
    
    def linkgen(self):
        start = "03-Apr-2006"
        end = (datetime.today()- timedelta(days=1)).strftime("%d-%b-%Y")
        date = self.split_date(start, end)
        links = []
        for a,b in date:
            url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?&frmdt={a.strftime('%d-%b-%Y')}&todt={b.strftime('%d-%b-%Y')}"
            links.append(url)
        return links

    def check_db(self, dbname: str) -> bool:
        a = self.cnx.cursor()
        a.execute("SHOW DATABASES;")
        dbl = []
        for n in a:
            dbl.append(n[0])
        if dbname not in dbl:
            print("i did execute")
            a.execute(f"CREATE DATABASE IF NOT EXISTS {dbname};")
        self.cnx.close()
        

    async def get_corrupted_data(self):
        async with aiohttp.ClientSession() as session:
            result = [session.get(i) for i in self.links]
            response = await asyncio.gather(*result)
            ri = []
            for i in response:
                print(i.status)
                ri.append(await i.text())
        return ri
    
    def transform(self, txt):
        data = txt.split("\r\n")
        
    
        
    # def get_data():
        
    
aa = Err()
aa.check_db("dbname")
ts = datetime.now()
txt = asyncio.run(aa.get_corrupted_data())
te = datetime.now()
print(txt)
ti = te-ts
print(ti)