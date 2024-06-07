from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("avrgoflist").getOrCreate()

l=[10,22,30,14,14,43]

rdd = spark.sparkContext.parallelize(l)
rdd.collect()

df=rdd.sum()
df1=rdd.count()

avarage=df/df1

print(f"the avarge is:{avarage}")