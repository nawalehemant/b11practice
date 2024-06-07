from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("practice").getOrCreate()

df=spark.read.format("json").\
    option("multiline","true").\
    load("file:///Users/hemantvilasnawale/Documents/offeline/spark/Json.file/sample1.json")


df.printSchema()

df.show()