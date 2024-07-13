from Tablular_data import MFRecommand
from datetime import datetime

aa = MFRecommand()
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
'''For future self
I have chnaged the path of Data dir of MYSQL from D:/ drive to C:/ drive so that the data can be stored and read fast. 
Later plan is to move that data file to D:/ drive.
what is life? i dont know.
'''