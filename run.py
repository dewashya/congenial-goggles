from Tablular_data import Err
from datetime import datetime, timedelta
import asyncio
import pandas

aa = Err()
aa.check_db("dbname")
ts = datetime.now()
txt = asyncio.run(aa.get_corrupted_data())
te = datetime.now()
print(txt)
ti = te-ts
print(ti)
data = aa.transform(txt)

print(data[0])

