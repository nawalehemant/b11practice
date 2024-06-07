from pyspark.sql import  SparkSession
from pyspark.sql.functions import *
#(nested file)
spark=SparkSession.builder.appName("Sample").getOrCreate()

from pyspark.sql.functions import *

df=spark.read.format("JSON").option("multiline","true").\
    load("file:///Users/hemantvilasnawale/Documents/offeline/spark/Json.file/nes .json")

df1=df.withColumn("source",explode(array("source.*"))).\
    withColumn("id",col("source.id")).\
    withColumn("ip",col("source.ip")).\
    withColumn("temp",col("source.temp")).\
    withColumn("description",col("source.description")).\
    withColumn("c02_level",col("source.c02_level")).\
    withColumn("lat",col("source.geo.lat")).\
    withColumn("long",col("source.geo.long")).drop("source")


#df1.printSchema()

#df1.show()

df1.select("ip","id").show()