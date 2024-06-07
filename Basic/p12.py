from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("practice").getOrCreate()


def json_read(spark,path,multiline=False):
    df=spark.read.format("json").option("multiline",multiline).load(path)
    return df

df1=json_read(spark,"/Users/hemantvilasnawale/Documents/offeline/spark/Json.file/cars .json")

df1.show()

def json_read(spark,path,multiline=False):
    df=spark.read.format("json").option("multiline",multiline).load(path)
    return df

df2=json_read(spark,"/Users/hemantvilasnawale/Documents/offeline/spark/Json.file/sample1.json",'true')

df2.show()
