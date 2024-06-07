from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("practice").getOrCreate()

df=spark.read.format("json").option("Multiline","true").\
    load("file:///Users/hemantvilasnawale/Documents/offeline/spark/Json.file/sample4.json")


df1=df.withColumn("people",explode(df["people"])).\
    withColumn("age",col("people.age")).\
    withColumn("firstName",col("people.firstName")).\
    withColumn("gender",col("people.gender")).\
    withColumn("lastName",col("people.lastName")).\
    withColumn("number",col("people.number")).drop("people")


df1.write.format("csv").option("header","true").\
    save("file:///Users/hemantvilasnawale/Documents/offeline/spark/Json.file/sample4.csv")

df1.printSchema()

df1.show()