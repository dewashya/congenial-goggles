"""
In this Script I try to extract Error NAV value and Scheme Code

The main problem is in NAV there are some error value which is not the ideal float value. i want to figure out those rows and 
keep them to refer again while cleaning the data. 
"""
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import aiohttp
import mysql.connector
from sqlalchemy import create_engine
from mysql.connector import errorcode
import hashlib




class Err:
    def __init__(self):
        self.user = 'root'
        self.password = 'De$#Ka@)*01Zz'
        self.host = "localhost"
        self.dbname = "dbname"
        self.port = 3306
        self.cnx = mysql.connector.connect(user=self.user, password=self.password)
        if self.cnx.is_connected() == True:
            print("Connection made!")
        self.check_db(self.dbname)
        self.links = self.linkgen()
        # print(self.links)


    def split_date(self, start_date, end_date, ran=90)-> list:
        start_date= datetime.strptime(start_date, "%d-%b-%Y")
        end_date = datetime.strptime(end_date, "%d-%b-%Y")
        
        date_ranges = []
        if start_date == end_date:
            date_ranges.append((start_date, end_date))
            return date_ranges
        range_start = start_date
        
        while range_start < end_date:
            range_end = range_start + timedelta(days=ran - 1)
            
            if range_end > end_date:
                
                range_end = end_date

            date_ranges.append((range_start, range_end))
            range_start = range_end + timedelta(days=1)
        
        return date_ranges
    
    def linkgen(self):
        checkstart = self.get_latest_date(self.dbname)
        # print(checkstart)
        if  checkstart == None:
            start = "03-Apr-2006"
        else:
            start = checkstart
        end = (datetime.today()- timedelta(days=1)).strftime("%d-%b-%Y")
        date = self.split_date(start, end)
        links = []
        for a,b in date:
            url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?&frmdt={a.strftime('%d-%b-%Y')}&todt={b.strftime('%d-%b-%Y')}"
            links.append(url)
        return links


    def check_db(self, dbname: str):
        try:
            a = self.cnx.cursor()
            a.execute("SHOW DATABASES;")
            dbl = [n[0] for n in a]
            if dbname not in dbl:
                print("Database does not exist. Creating database...")
                a.execute(f"CREATE DATABASE {dbname};")
                return print(f"Database {dbname} created successfully.")
            else:
                return print(f"Database {dbname} already exists.")
        finally:
            a.close()
            print("Database exsists")
    
    def get_latest_date(self, dbname: str):
        try:
            self.cnx.database = dbname
            cursor = self.cnx.cursor()

            # Check if the table exists
            cursor.execute(f"SHOW TABLES LIKE 'MutualFund';")
            result = cursor.fetchone()
            if result:
                # Table exists, get the latest date
                cursor.execute("SELECT MAX(Date) FROM MutualFund;")
                latest_date = cursor.fetchone()[0]
                if latest_date:
                    formatted_date = datetime.strptime(str(latest_date), '%Y-%m-%d').strftime('%d-%b-%Y')
                    print(f"Latest date in 'MutualFund' table is: {formatted_date}")
                return formatted_date
            else:
                print("Table 'MutualFund' does not exist.")
                return None
        finally:
            cursor.close()
            print("latest date checked")
            # print("Closing connection...")
            # self.cnx.close()
        
    def insert_dataframe(self, dbname: str, table_name: str, data):
        try:
            cursor = self.cnx.cursor()

            # Check if the table exists
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.tables 
                WHERE table_schema = '{dbname}' 
                AND table_name = '{table_name}'
            """)
            if cursor.fetchone()[0] == 0:
                # Create the table if it doesn't exist
                self.create_table(dbname, table_name)

            # Append data to the table
            sql = f"""
                INSERT INTO {dbname}.{table_name} (Structure, Category, Sub_Category, AMC, Code, Name, Inv_Source, Option_type, Date, NAV, Hash_Column)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    NAV = VALUES(NAV)
            """

            for row in data:
                # Compute hash for relevant columns (excluding NAV and Hash_Column)
                hash_columns = row[:-1]  # Exclude NAV and Hash_Column
                hash_value = self.create_hash(hash_columns)
                
                # Append hash value to row
                row = row + (hash_value,)
                
                # Execute SQL with row data
                cursor.execute(sql, row)

            # Commit the transaction
            self.cnx.commit()
            print(f"{cursor.rowcount} rows were inserted into '{table_name}'.")

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        finally:
            cursor.close()
            # No need to close connection here if you intend to reuse the handler for other operations

    def create_table(self, dbname, table_name):
        try:
            cursor = self.cnx.cursor()

            # Create the table with a primary key and hash column
            cursor.execute(f"""
                CREATE TABLE {dbname}.{table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    Structure VARCHAR(255),
                    Category VARCHAR(255),
                    Sub_Category VARCHAR(255),
                    AMC VARCHAR(255),
                    Code INT,
                    Name VARCHAR(255),
                    Inv_Source VARCHAR(255),
                    Option_type VARCHAR(255),
                    Date DATE,
                    NAV VARCHAR(255),
                    Hash_Column VARCHAR(64),
                    UNIQUE (Hash_Column)
                )
            """)
            print(f"Table '{table_name}' created successfully.")

            cursor.close()

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def create_hash(self, columns):
        concat_string = '|'.join(map(str, columns))  # Concatenate columns into a string
        hash_object = hashlib.sha256(concat_string.encode())  # Create SHA-256 hash
        return hash_object.hexdigest()  # Return hash as hexadecimal string

    def close_connection(self):
        if self.cnx:
            self.cnx.close()
            print("Connection closed.")

            
    async def get_data(self,l):
        async with aiohttp.ClientSession() as session:
            result = [session.get(i) for i in l]
            response = await asyncio.gather(*result, return_exceptions=True)
            ri = []
            re = []
            required_text = "Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date"
            for i, resp in enumerate(response):
                if isinstance(resp, Exception):
                    re.append(l[i])
                    print(f"Error fetching {l[i]} : {resp}")
                else:
                    print(resp.status)
                    try:
                        text = await resp.text()
                        if required_text in text:
                            ri.append(text)
                        else:
                            re.append(l[i])
                            print(f"Required text not found in {l[i]}")
                    except Exception as e:
                        re.append(l[i])
                        print(f"Error processing {l[i]} : {e}")
        print(f"Out of {len(l)} / {len(ri)} gave response")
        return ri,re
    
# Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date
    def getall(self):
        lin = self.links
        data = []
        while int(len(lin)) != 0:
            a,b = asyncio.run(self.get_data(lin))
            # print(a)
            lin = b
            data.append(a)
            # print(len(lin))
        return data

    def unrap(self, string):
        txt = [j for i in string for j in i]
        a = len(txt)
        print(f"the size of data is {a}")
        return txt

    def transform(self,i):
        all = []
        i = i.splitlines()
        n = 1
        for j in i[1:]:
            l = j.split(";")
            if n == len(i)-1:
                break
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
                date = datetime.strptime(l[7], "%d-%b-%Y").strftime('%Y-%m-%d')
                nav = l[4].strip()
                # new_record = {
                #         "Structure": Structure,
                #         "Category": Category, 
                #         "Sub-Category": Sub_Category,
                #         "AMC": amc, 
                #         "Code": code, 
                #         "Name": name,
                #         "Source": inv_src,
                #         "Option" : dg,
                #         "date":date, 
                #         "nav": nav
                # }
                new_record = (Structure, Category, Sub_Category, amc, code, name, inv_src, dg, date, nav)
                all.append(new_record)
            n+=1
        return all
                    
                    
                        
        
    
        
    
