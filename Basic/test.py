from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from functions import *

spark=SparkSession.builder.appName("Brodcast Join").getOrCreate()

df_emp=read_csv(spark,"/Users/hemantvilasnawale/Desktop/Salting")
#df_dept=read_csv(spark,"C:/Users/Lenovo/Desktop/dept.csv",delimiter=",")
#df_nes=read_json(spark,"C:/Users/Lenovo/Desktop/Hemant/nes.json",multiline='true')


df_emp.show()
#df_dept.show()
#df_nes.show()
