from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("avrgoflist").getOrCreate()

l=[1,2,3,4,5]

rdd = spark.sparkContext.parallelize(l)
rdd.collect()

df=rdd.sum()
df1=rdd.count()

avarage=df/df1

print(f"the avarge is:{avarage}")