from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Spark Test").getOrCreate()

ls=[10,20,30,40]

rdd=spark.sparkContext.parallelize(ls)

print(rdd.collect())



rdd1=rdd.repartition(4)

print(rdd1.getNumPartitions())

