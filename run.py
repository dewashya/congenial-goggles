from Tablular_data import Err
from datetime import datetime, timedelta
import asyncio
import pandas

aa = Err()
aa.check_db("dbname")
ts = datetime.now()
txt = aa.getall()
te = datetime.now()
print(txt[:-500])
ti = te-ts
print(ti)
# with open('output.txt', 'w') as f:
#     for item in txt:
#         f.write("%s\n\n" % item)
data = aa.transform(txt)

final_data = pandas.DataFrame.from_dict(data[0])
clean_data=pandas.DataFrame.from_dict(data[1])
error_data = pandas.DataFrame.from_dict(data[2])

aa.insert_dataframe("dbname", final_data, "Complete")
aa.insert_dataframe("dbname", clean_data, "Clean")
aa.insert_dataframe("dbname", error_data, "Error")



