from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import os
from functions import *
from datetime import date
today_date = date.today()
spark=SparkSession.builder.appName("Gerneric-fun").getOrCreate()


input_path="E:/Script/config.json"

with open(input_path,'r') as file:
    data=json.load(file)

file.close()
input = data.get('input_data', '')
output= data.get('output_data','')
df=read_csv(spark,input)
df1=df.filter(upper(df['Genre'])=='ACTION')
df1.write.format("csv").mode("overwrite").save(output)
src_cnt=df.filter(df['Genre']=='Action').count()
trg_cnt=df.filter(upper(df['Genre'])=='ACTION').count()
if src_cnt==trg_cnt:
    print("Record count are passed")
    cstatus="pass"
else:
    print ("Record count are Not passed")
    cstatus = "Failed"

src_data=df.dtypes
trg_data=df1.dtypes

if src_data==trg_data:
    print("Record datatypes are passed")
    dstatus = "pass"
else:
    print ("Record datatypes are Not passed")
    dstatus = "Failed"

with open("hh.txt","w") as file:
    file.write("data as todays {}\n".format(today_date))
    file.write("Source Count : {} \t Target Count : {} \n Status is {}\n".format(src_cnt,trg_cnt,cstatus))
    file.write("Source DataType : {} \t Target DataType : {} \n Status is {}\n".format(src_data, trg_data, dstatus))
file.close()

print("##### complete ############")