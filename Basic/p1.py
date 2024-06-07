from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("practice").getOrCreate()

df=spark.read.format("json").\
    option("multiline","true").\
    load("file:///Users/hemantvilasnawale/Documents/offeline/spark/Json.file/sample2.json")

df1=df.withColumn("address",explode(array("address.*"))).\
    withColumn("number",explode("phoneNumbers.number")).\
    withColumn("type",explode("phoneNumbers.type")).drop("phoneNumbers")

df1.write.format("json").option("header","true").\
    save("file:///Users/hemantvilasnawale/Documents/offeline/spark/Json.file/sampleans2.json")

df1.printSchema()
df1.show()