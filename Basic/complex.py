from pyspark.sql import  SparkSession
spark=SparkSession.builder.appName("Sample").getOrCreate()

from pyspark.sql.functions import *

df=spark.read.format("JSON").option("multiline","true").\
    load("file:///Users/hemantvilasnawale/Documents/offeline/spark/Json.file/persons.json")

df1=df.withColumn("persons",explode(df['persons'])).\
    withColumn("cars",explode("persons.cars")).\
    withColumn("models",explode("cars.models")).\
    selectExpr("persons.name","persons.age","cars.name as car_name","models")


df2=df1.groupby("name").count()

#df.printSchema()

df2.show(20,False)