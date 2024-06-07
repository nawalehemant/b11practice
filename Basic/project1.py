from pyspark import StorageLevel
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Spark Test").getOrCreate()

rdd=spark.sparkContext.textFile("file:////Users/hemantvilasnawale/Desktop/email.txt")
rdd1=rdd.flatMap(lambda x:x.split(","))
rdd2=rdd1.map(lambda x:x.split("@")[1])
rdd3=rdd2.map(lambda x:(x,1))
rdd4=rdd3.reduceByKey(lambda x,y:x+y)
print(rdd4.collect())

rdd2.persist(StorageLevel.DISK_ONLY)




