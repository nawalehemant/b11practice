from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("practice").getOrCreate()


df=spark.read.format("json").option("multiline","true").load("file:///Users/hemantvilasnawale/Documents/offeline/spark/Json.file/s2.json")


df1=df.withColumn("problems",explode("problems")).\
    withColumn("Diabetes",explode("problems.Diabetes")).\
    withColumn("labs",explode("Diabetes.labs")).\
    withColumn("missing_field",col("labs.missing_field")).\
    withColumn("medications",explode("Diabetes.medications")).\
    withColumn("medicationsClasses",explode("medications.medicationsClasses")).\
    withColumn("className",explode("medicationsClasses.className")).\
    withColumn("associatedDrug",explode("className.associatedDrug")).\
    withColumn("dose",col("associatedDrug.dose")).\
    withColumn("name",col("associatedDrug.name")).\
    withColumn("strength",col("associatedDrug.strength")).\
    withColumn("associatedDrug#2",explode("className.associatedDrug#2")).\
    withColumn("dose",col("associatedDrug#2.dose")).\
    withColumn("name",col("associatedDrug#2.name")).\
    withColumn("strength",col("associatedDrug#2.strength")).\
    withColumn("className2",explode("medicationsClasses.className2")).\
    withColumn("associatedDrug",explode("className2.associatedDrug")).drop("missing_field").drop("medicationsClasses ").drop("associatedDrug ").drop("associatedDrug#2").\
    drop("problems").drop("Diabetes")



df1.printSchema()
df1.show(20,False)