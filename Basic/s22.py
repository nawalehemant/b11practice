from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("practice").getOrCreate()


df=spark.read.format("json").option("multiline","true").load("file:///Users/hemantvilasnawale/Documents/offeline/spark/Json.file/s2.json")

#df.printSchema()
#df.show()

df1=df.withColumn("problems",explode(array("problems"))).\
    withColumn("Diabetes",explode("problems.Diabetes")).drop("problems").\
    withColumn("labs",explode("Diabetes.labs")).\
    withColumn("medications",explode("Diabetes.medications")).\
    withColumn("medicationsClasses",explode("medications.medicationsClasses"))



df1.printSchema()
df1.show(20,False)