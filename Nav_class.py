import time

start_time = time.time()

import requests
from pytz import utc
from datetime import datetime, timedelta
import pymongo

class NAVUpdater:
    def __init__(self, mongo_uri, database_name, nav_collection_name, meta_collection_name):
        self.mongo_client = pymongo.MongoClient(mongo_uri)
        self.mydb = self.mongo_client[database_name]
        self.nav_collection = self.mydb[nav_collection_name]
        self.meta_collection = self.mydb[meta_collection_name]

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

    def fetch_and_store_nav_data(self, start, end):
        url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?&frmdt={start}&todt={end}"
        response = requests.session().get(url)
        print("Data fetched from the connection")

        data = response.text.split("\r\n")
        structure = ""
        category = ""
        sub_category = ""
        amc = ""
        code = int()
        name = str()
        nav = float()
        date = ""
        inv_src = ""
        dg = ""
        j = 1

        nav_data_list = []
        for lines in data[1:]:
            split = lines.split(";")

            if j == len(data) - 1:
                break

            if split[0] == "":
                if data[j] == data[j + 1]:
                    sch_cat = data[j - 1].split("(")
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

                elif "Mutual Fund" in data[j + 1]:
                    amc = data[j + 1]

            elif len(split) > 1:
                code = int(split[0].strip())
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
                    nav = split[4].strip()

                date = self.convert_date_to_utc_datetime(split[7].strip())
                print(type(date), date)

                nav_data_list.append({
                    "date": date,
                    "code": code,
                    "nav": nav
                })

                existing_data = self.meta_collection.find_one({"meta.Code": code})

                if existing_data:
                    self.meta_collection.update_one({"_id": existing_data["_id"]}, {
                        "$set": {"meta.Structure": structure,
                                 "meta.Category": category,
                                 "meta.Sub-Category": sub_category,
                                 "meta.AMC": amc,
                                 "meta.Code": code,
                                 "meta.Name": name,
                                 "meta.Source": inv_src,
                                 "meta.Option": dg}
                    })
                    print("Existing metadata updated")
                else:
                    new_meta_record = {
                        "meta": {
                            "Structure": structure,
                            "Category": category,
                            "Sub-Category": sub_category,
                            "AMC": amc,
                            "Code": code,
                            "Name": name,
                            "Source": inv_src,
                            "Option": dg
                        }
                    }
                    self.meta_collection.insert_one(new_meta_record)
                    print("New metadata inserted")

            j = j + 1

        if nav_data_list:
            self.nav_collection.insert_many(nav_data_list)
            print("Nav data inserted")

    def process_date_range(self, start_date_str, end_date_str, max_duration=90):
        date_ranges = self.split_date_range(start_date_str, end_date_str, max_duration)

        for start, end in date_ranges:
            print(f"Fetching NAV data for {start.strftime('%d-%b-%Y')} to {end.strftime('%d-%b-%Y')}")
            self.fetch_and_store_nav_data(start.strftime('%d-%b-%Y'), end.strftime('%d-%b-%Y'))

if __name__ == "__main__":
    # Example usage
    mongo_uri = "mongodb://localhost:27017/"
    database_name = "M_F2"
    nav_collection_name = "NavData"
    meta_collection_name = "MetaData"

    nav_updater = NAVUpdater(mongo_uri, database_name, nav_collection_name, meta_collection_name)

    start_date_str = "03-Apr-2023"
    end_date_str = (datetime.utcnow() - timedelta(days=1)).strftime('%d-%b-%Y')
    max_duration = 90

    nav_updater.process_date_range(start_date_str, end_date_str, max_duration)
    input("Press any key to confirm")


end_time = time.time()
print(f"Total runtime: {end_time - start_time} seconds")