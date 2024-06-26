"""
In this Script I try to extract Error NAV value and Scheme Code

The main problem is in NAV there are some error value which is not the ideal float value. i want to figure out those rows and 
keep them to refer again while cleaning the data. 
"""
import asyncio
from datetime import datetime, timedelta
import aiohttp
import mysql.connector



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
        all = []
        final = []
        error = []
        for i in txt:
            i= i.split("\r\n")
            n = 1
            for j in i[1:]:
                l = j.split(";")
                if l[0] == "":
                    if i[n] == i[n+1]:
                        sch_cat = i[n-1].split("(")
                        sch_cat[-1]=sch_cat[-1][:-2].strip()
                        sch_cat = [i.strip() for i in sch_cat]
                        if "-" in sch_cat[1]:
                            sch_sub_cat = sch_cat[1].split("-")
                            sch_sub_cat = [i.strip() for i in sch_sub_cat]
                            sch_cat.pop(-1)
                            sch_cat = sch_cat+sch_sub_cat
                        else:
                            sch_sub_cat = ["",sch_cat[1]]
                            sch_cat.pop(-1)
                            sch_cat = sch_cat+sch_sub_cat 
                        Structure = sch_cat[0]
                        Category = sch_cat[1]
                        Sub_Category = sch_cat[2]
                    elif "Mutual Fund" in i[n+1]:
                        amc = i[n+1]
                elif len(l)>1:
                    code = int(l[0].strip())
                    name = str(l[1].strip())
                    if "growth" in name.lower():
                        dg = "Growth"
                    elif "idcw" or "dividend" in name.lower():
                        dg = "IDCW"
                    else:
                        dg = ""

                    if "direct" in name.lower():
                        inv_src = "Direct"
                    elif "regular" in name.lower():
                        inv_src = "Regular"
                    else:
                        inv_src = ""
                    date = datetime.strptime(l[7], "%d-%b-%Y")
                    try:
                        nav = float(l[4].strip()) 
                        new_record = {
                                "Structure": Structure,
                                "Category": Category, 
                                "Sub-Category": Sub_Category,
                                "AMC": amc, 
                                "Code": code, 
                                "Name": name,
                                "Source": inv_src,
                                "Option" : dg,
                                "date":date, 
                                "nav": nav
                        }
                        final.append(new_record)
                        all.append(new_record)    
                    except:
                        print(f"This is not a float nav value {l[4]}")
                        nav = l[4].strip()
                        new_record = {
                                "Structure": Structure,
                                "Category": Category, 
                                "Sub-Category": Sub_Category,
                                "AMC": amc, 
                                "Code": code, 
                                "Name": name,
                                "Source": inv_src,
                                "Option" : dg,
                                "date":date, 
                                "nav": nav
                        }
                        error.append(new_record)
                        all.append(new_record)
                n+=1
        return [all, final, error]
                    
                    
                        
        
    # def get_data():
        
    
