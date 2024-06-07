from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("practice").getOrCreate()

df=spark.read.format("json").\
    load("file:///Users/hemantvilasnawale/Documents/offeline/spark/Json.file/cars .json")

df.printSchema()
df.show(100)

df1=df.where(df["kind"]=="electric")

df1.show()