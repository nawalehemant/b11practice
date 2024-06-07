from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("HDFS to HIVE").enableHiveSupport().getOrCreate()

df=spark.read.format("csv").option("header","true").option("inferschema","true").load("/hemant.csv")
df.show()

df.write.saveAsTable("hemant")