from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("FileHandling").getOrCreate()
from pyspark.sql.functions import *
from functions import read_csv
import json
from pyspark.sql.functions import upper
from datetime import *

config_file_path="/content/drive/MyDrive/Colab Notebooks/config.json"

#config file data load without function-
with open(config_file_path,"r") as new:
  xyz=json.load(new)

input=xyz.get("input_path")
output=xyz.get("output_path")

#config file data load with function-
def config_file_data(json_file):
  with open(config_file_path,"r") as new:
     xyz=json.load(new)
  return xyz

config_data=config_file_data(config_file_path)

input_path=config_data.get("input_path")
output_path=config_data.get("output_path")

#create df-
df=read_csv(spark,input_path,",")
#df.show(5)

#process data-
df1=df.filter(upper(col("Genre"))=="ACTION")
#df1.show(5)

#write file-
df1.write.mode("overwrite").csv(output_path,header=True)


#Validation-
source=read_csv(spark,input_path)
target=read_csv(spark,output_path)

#source.show()
#target.show()

#1.Count-

source_count=source. filter(upper(col("Genre"))=="ACTION").count()
target_count=target.count()

if source_count==target_count:
  print("count validation Passed")
  scount="Pass"
else:
  print("count validation Failed")
  scount="Failed"

#2.Schema-
source_schema=source.dtypes
target_schema=target.dtypes

if source_schema==target_schema:
  print("Schema validation Passed")
  sschema="Pass"
else:
  print("Schema validation Failed")
  sschema="Failed"

#print(source.dtypes)
#print(target.dtypes)

def schema_handling(list):
  list=str(list).replace("decimal(5,0)","int")
  return list

source_schema=schema_handling(source_schema)

target_schema=target.dtypes
target_schema=str(target_schema)

if source_schema==target_schema:
  print("Schema validation Passed")
  sschema="Pass"
else:
  print("Schema validation Failed")
  sschema="Failed"

with open("/content/drive/MyDrive/Colab Notebooks/rhushikesh/vailed.txt","w") as val_file:
  val_file.write("## validation- \n")
  val_file.write("  -validation report genrated date: {} \n".format(datetime.today()))

  val_file.write("#1.Count Validation- \n")
  val_file.write("  -source_count: {} \n".format(source_count))
  val_file.write("  -target_count: {} \n".format(target_count))
  val_file.write("  -Count validation: {} \n".format(scount))

  val_file.write("#2.Schema Validation- \n")
  val_file.write("  -source_Schema: {} \n".format(source_schema))
  val_file.write("  -target_Schema: {} \n".format(target_schema))
  val_file.write("  -Schema vaildation: {} \n".format(sschema))

  val_file.write("## Vaildation Successfully \n")