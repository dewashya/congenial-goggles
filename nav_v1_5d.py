#https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?&frmdt=14-Aug-2023&todt=16-Aug-2023
#so above is the link for the site
import requests
from pytz import utc
from datetime import datetime
import pymongo  # Import the pymongo library for MongoDB operations

#im trying to post it on git

# Initialize MongoDB client and database
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB connection string
mydb = mongo_client["M_F"]  # Replace with your database name
mycollection = mydb["MyNAV"]  # Replace with your collection name


def convert_date_to_utc_datetime(date_string):
    date_format = "%d-%b-%Y"
    date_object = datetime.strptime(date_string, date_format)
    return date_object.replace(tzinfo=utc)

#using GPT i dont have time to do this bro.
from datetime import datetime, timedelta

def split_date_range(start_date_str, end_date_str, max_duration=90):
    # Convert input date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%d-%b-%Y")
    end_date = datetime.strptime(end_date_str, "%d-%b-%Y")

    date_ranges = []

    current_date = start_date
    while current_date <= end_date:
        # Calculate the end of the current sub-range
        sub_range_end = current_date + timedelta(days=max_duration - 1)
        
        # Make sure the sub-range end is not greater than the end_date
        if sub_range_end > end_date:
            sub_range_end = end_date

        # Append the current sub-range as a tuple to the list
        date_ranges.append((current_date, sub_range_end))

        # Move the current_date to the day after the sub-range end
        current_date = sub_range_end + timedelta(days=1)

    return date_ranges

# Example usage
# start_date_str = "03-Apr-2006"
# end_date_str = "03-Apr-2023"
# max_duration = 90

# date_ranges = split_date_range(start_date_str, end_date_str, max_duration)
# # Printing the resulting date ranges
# for start, end in date_ranges:
#     print(f"Start Date: {start.strftime('%d-%b-%Y')}, End Date: {end.strftime('%d-%b-%Y')}")

def nav_data(start,end):
    """Put the date in DD-Mmm-YYYY that too in a string format"""
    url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?&frmdt={start}&todt={end}"
    response = requests.session().get(url)
    print("Got the data form connection")
    data = response.text.split("\r\n")
    # scheme_info = {"meta": {}, "data": []} 
    Structure = ""
    Category = ""
    Sub_Category = ""
    amc = ""
    code = int()
    name = str()
    nav = float()
    date = ""
    inv_src = ""
    dg = ""
    j = 1
    
    for lines in data[1:]:
        split = lines.split(";")
        #So i am starting the data from index 1 of data.
        if j == len(data)-1:
            break
        if split[0] == "":
            # To check the Scheme [Structure, Category, Sub-Category]
            if data[j] == data[j+1]:
                #Open Ended Schemes ( Equity Scheme - Mid Cap Fund )
                sch_cat = data[j-1].split("(")
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
                #print(sch_cat)
            # to check the AMC name
            elif "Mutual Fund" in data[j+1]:
                amc = data[j+1]
        elif len(split)>1:
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
                print(f"This is not a float nav value {split[4]}")
                nav = split[4].strip()
           
            date = convert_date_to_utc_datetime(split[7].strip())
            print(type(date),date)
            existing_data = mycollection.find_one({"meta.Code": code})
            # print(existing_data)
            # date_existing = mycollection.find_one({"data.date": date})
            # print(date_existing)
            if existing_data:
                    #now i should do insert many
                # the below part is not working gpt
                # if date_existing:
                #     print("Its there")
                #     continue
                # # work on the above one
                # else:
                    # If data with the code already exists in MongoDB, update it
                    mycollection.update_one({"_id": existing_data["_id"]}, {
                        "$push": {"data": {"date": date, "nav": nav}}})
                    print("Another one bites the dust")
            else:
                new_record = {
                    "meta": {
                        "Structure": Structure,
                        "Category": Category, 
                        "Sub-Category": Sub_Category,
                        "AMC": amc, 
                        "Code": code, 
                        "Name": name,
                        "Source": inv_src,
                        "Option" : dg
                    },
                    "data": [{"date":date, "nav": nav }]
                }
                # scheme_info["meta"]={
                #     "Structure": Structure,
                #     "Category": Category, 
                #     "Sub-Category": Sub_Category,
                #     "AMC": amc, 
                #     "Code": code, 
                #     "Name": name
                #     }
                # scheme_info["data"] = [{"date":date,
                #                         "nav": nav }]
                # print(new_record.keys())
                mycollection.insert_one(new_record)
                print("Data data data")
        # print(j)
        j = j+1

    return

start_date_str = "04-Apr-2023"
end_date_str = "31-Aug-2023"
max_duration = 90

date_ranges = split_date_range(start_date_str, end_date_str, max_duration)
for start, end in date_ranges:
    print(f"Start Date: {start.strftime('%d-%b-%Y')}, End Date: {end.strftime('%d-%b-%Y')}")
    nav_data(start.strftime('%d-%b-%Y'),end.strftime('%d-%b-%Y'))
input("press any key to confirm")

# {'meta': {'Structure': 'Open Ended Schemes ',
#            'Category': '',
#            'Sub-Category': 'Income',
#            'AMC': 'ABN  AMRO Mutual Fund',
#            'Code': 102653, 
#            'Name': 'ABN AMRO  Monthly Income Plan-Regular Plan-Monthly Dividend Option'}, 
#            'data': [{'date': datetime.datetime(2006, 4, 3, 0, 0, tzinfo=<UTC>), 'nav': 10.5045}], 
#            #I did not feed this last part i dont know from where did it came from.
#            '_id': ObjectId('64ec02b43c800abccf65925a')}

# from datetime import datetime, tzinfo, timezone
# from pymongo import MongoClient

# # Requires the PyMongo package.
# # https://api.mongodb.com/python/current

# client = MongoClient('mongodb://localhost:27017/')
# filter={
#     'data.date': {
#         '$gte': datetime(2010, 1, 1, 0, 0, 0, tzinfo=timezone.utc), 
#         '$lte': datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
#     }
# }

# result = client['M_F']['MyNAV'].find(
#   filter=filter
# )