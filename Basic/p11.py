from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("practice").getOrCreate()


def csv_read(spark,path,delimiter=','):
  df=spark.read.format("csv").option("delimiter",delimiter).option("inferschema","true").\
      option("header","true").load(path)
  return df



df=csv_read(spark,"/Users/hemantvilasnawale/Desktop/emp.csv")

df.show(20,False)