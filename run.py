from Tablular_data import Err
from datetime import datetime

aa = Err()
ts = datetime.now()
txt = aa.getall()
te = datetime.now()
# print(txt)
ti = te-ts
print(ti)

data = aa.unrap(txt)
print("Unwrap list completed")

o = 1
for d in data:
    data = aa.transform(d)
    print("Tranformer🤖", o)
    aa.insert_data("dbname", "MutualFund", data)
    print(f"{o} data is inserted completely")
    o += 1

