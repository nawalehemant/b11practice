from pyspark.sql import  SparkSession
spark=SparkSession.builder.appName("Sample").getOrCreate()

df=spark.read.format("csv").option("header","true").option("delimiter","|")\
    .option("inferschema","true")\
    .load("file:///Users/hemantvilasnawale/Desktop/emp.csv")

df1=df.where(df['did']==10)
df1.show()
df2=df1.where(df1['sal']>30000)

df2.show()

df2.write.format("csv").save("file:///Users/hemantvilasnawale/Desktop/empop ")