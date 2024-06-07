from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("HDFS to Hbase").getOrCreate()

df=spark.read.format("csv").option("header","true").option("delimited",",").option("inferschema","true").load("/hemant1/part-00000-d357c2a9-8894-427a-a964-d707b32bc4b6-c000.csv")

df.show()

df.write.format("org.apache.phoenix.spark").option("zkurl", "localhost:2181").mode("overwrite").option("table", "HD_HEMANT").save()