from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Spark").getOrCreate()

df_t1 = spark.read.format("csv").option("header", "true"). \
    option("delimiter", ","). \
    load("file:///Users/hemantvilasnawale/Documents/t1.csv")

df_t1.show()

df_t2 = spark.read.format("csv").option("header", "true"). \
    option("delimiter", ","). \
    load("file:///Users/hemantvilasnawale/Documents/t2.csv")

df_t2.show()
#FOR CHANGE PARTITION

df_t1=df_t1.repartition(3)
df_t2=df_t2.repartition(3)


#CREAT VIEW
df_t1.createOrReplaceTempView("t1")
df_t2.createOrReplaceTempView("t2")

df_res=spark.sql("""select * from t1 union select * from t2""")
df_res.show()

#df_res.write.format("csv").save("file:///Users/hemantvilasnawale/Documents/resset.csv")

df_res1=spark.sql("""select * from t1 intersect select * from t2""")
df_res1.show()

#df_res1.write.format("csv").save("file:///Users/hemantvilasnawale/Documents/resset1.csv")

df_res2=spark.sql("""select * from t1 minus select * from t2""")
df_res2.show()

#df_res2.write.format("csv").save("file:///Users/hemantvilasnawale/Documents/resset2.csv")