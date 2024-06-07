from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("UDF function").getOrCreate()

df=spark.read.format("csv").option("header","true").option("delimitede",",").option("inferschema","true").\
    load("file:///C:/Users/Lenovo/Desktop/s1.csv")

#df.show()

def total_revenue(quantity,unit_price):
    return quantity*unit_price

df=df.withColumn("total_r",total_revenue(df["quantity"], df["unit_price"])).select("product_name","total_r")

#df2=df1.select("product_name","total_r")

df.show()
#df2.show()