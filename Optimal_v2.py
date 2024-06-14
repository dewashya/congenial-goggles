import time
import asyncio
import aiohttp
from pytz import utc
from datetime import datetime, timedelta
import pymongo
import logging


start_time = time.time()

class NAVUpdater:
    '''
    This module will help you to download and load Mutual Funds data from AMFI website.
    '''
    def __init__(self, mongo_uri, database_name, nav_collection_name, meta_collection_name):
        self.mongo_client = pymongo.MongoClient(mongo_uri)
        self.mydb = self.mongo_client[database_name]
        self.nav_collection = self.mydb[nav_collection_name]
        self.meta_collection = self.mydb[meta_collection_name]
        self.nav_data_list = []
        self.meta_data_list = []
        self.existing_data = set()
        self.logger = logging.getLogger(__name__)
        self.na_error = list()

    @staticmethod
    def convert_date_to_utc_datetime(date_string):
        date_format = "%d-%b-%Y"
        date_object = datetime.strptime(date_string, date_format)
        return date_object.replace(tzinfo=utc)

    @staticmethod
    def split_date_range(start_date_str, end_date_str, max_duration=90):
        start_date = datetime.strptime(start_date_str, "%d-%b-%Y")
        end_date = datetime.strptime(end_date_str, "%d-%b-%Y")

        date_ranges = []
        current_date = start_date

        while current_date <= end_date:
            sub_range_end = current_date + timedelta(days=max_duration - 1)

            if sub_range_end > end_date:
                sub_range_end = end_date

            date_ranges.append((current_date, sub_range_end))
            current_date = sub_range_end + timedelta(days=1)

        return date_ranges
    def splitnshape(data):
        '''
        I will try to Make my code more use-able. i will split the below code such that the spliting of data is 
        done here using different function. and this will return a list of values Maybe all the data or one by one idk.
        '''
        
        
    async def fetch_and_store_nav_data(self, start, end, session):
        url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?&frmdt={start}&todt={end}"
        async with session.get(url) as response:
            data = await response.text()
            self.logger.info("Data fetched from the connection")
            # Process and store data here (similar to the previous code)
            # structure = ""
            # category = ""
            # sub_category = ""
            # amc = ""
            # code = None
            # name = ""
            # nav = None
            # date = ""
            # inv_src = ""
            # dg = ""
            # I guess the above👆🏻 variable declaration was a waste.
            j = 1

            dut = data.split("\r\n")
            print(dut[0],dut[-1])
            self.logger.info(f"Number of lines in data: {len(dut)}")
            for lines in dut[1:]:
                split = lines.split(";")

                if j == len(dut) - 1:
                    break

                if split[0] == "":
                    if dut[j] == dut[j + 1]:
                        # for eg : Open Ended Schemes (Equity Scheme - Mid Cap Fund)
                        sch_cat = dut[j - 1].split("(")
                        sch_cat[-1] = sch_cat[-1][:-2].strip()
                        sch_cat = [i.strip() for i in sch_cat]

                        if "-" in sch_cat[1]:
                            sch_sub_cat = sch_cat[1].split("-")
                            sch_sub_cat = [i.strip() for i in sch_sub_cat]
                            sch_cat.pop(-1)
                            sch_cat = sch_cat + sch_sub_cat
                        else:
                            sch_sub_cat = ["", sch_cat[1]]
                            sch_cat.pop(-1)
                            sch_cat = sch_cat + sch_sub_cat

                        structure = sch_cat[0]
                        category = sch_cat[1]
                        sub_category = sch_cat[2]

                    elif "Mutual Fund" in dut[j + 1]:
                        amc = dut[j + 1]

                elif len(split) == 8:
                    try:
                        code = int(split[0].strip())
                    except:
                        self.na_error.append(split)
                        code = input(f"this is not right {code} do you want to put something there?")

                    name = str(split[1].strip())
                    
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

                    try:
                        nav = float(split[4].strip())
                    except:
                        self.na_error.append(split)
                        nav = split[4].strip()
                        
                    try:
                        date = self.convert_date_to_utc_datetime(split[7].strip())
                    except:
                        print(f"this {lines}")
                    
                    self.nav_data_list.append({
                        "date": date,
                        "Code": code,
                        "nav": nav
                    })

                    if code not in self.existing_data:
                        self.existing_data.add(code)
                        new_meta_record = {
                            "Structure": structure,
                            "Category": category,
                            "Sub-Category": sub_category,
                            "AMC": amc,
                            "Code": code,
                            "Name": name,
                            "Source": inv_src,
                            "Option": dg
                        }
                        self.meta_data_list.append(new_meta_record)
                        print(f"Data added to the list, count of j: {j}",end="\r")

                j = j + 1
        self.logger.info(f"Data gathered for {start} to {end}")

    def get_last_date_updated(self):
        last_data = self.nav_collection.find_one(sort=[("date", pymongo.DESCENDING)])

        if last_data:
            return last_data["date"]
        else:
            return None

    async def process_date_range_async(self):
        last_date_updated = self.get_last_date_updated()

        if last_date_updated:
            # Start from the next day after the last date updated
            start_date = last_date_updated + timedelta(days=1)
            start_date_str = start_date.strftime('%d-%b-%Y')
            end_date_str = (datetime.now(datetime.UTC) - timedelta(days=1)).strftime('%d-%b-%Y')
        else:
            # If there is no data, start from 3rd April 2006
            start_date_str = "03-Apr-2006"
            end_date_str = (datetime.now(datetime.UTC) - timedelta(days=1)).strftime('%d-%b-%Y')

        date_ranges = self.split_date_range(start_date_str, end_date_str)

        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_and_store_nav_data(start.strftime('%d-%b-%Y'), end.strftime('%d-%b-%Y'), session)
                     for start, end in date_ranges]

            await asyncio.gather(*tasks)

        self.logger.info("Now pushing Nav data")
        if self.nav_data_list:
            self.nav_collection.insert_many(self.nav_data_list)
            self.logger.info("Nav data inserted")
        self.logger.info("Now pushing Meta data")
        if self.meta_data_list:
            self.meta_collection.insert_many(self.meta_data_list)
            self.logger.info("Meta data inserted")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Example usage
    database_details = {
        "mongo_uri": "mongodb://localhost:27017/",
        "database_name": "M_F",
        "nav_collection_name": "NavData",
        "meta_collection_name": "MetaData"
    }

    nav_updater = NAVUpdater(
        database_details["mongo_uri"],
        database_details["database_name"],
        database_details["nav_collection_name"],
        database_details["meta_collection_name"]
    )

    asyncio.run(nav_updater.process_date_range_async())
    input("Press any key to confirm")

end_time = time.time()
print(f"Total runtime: {end_time - start_time} seconds")
