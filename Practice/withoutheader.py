from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName("Practice").getOrCreate()

header=StructType([
    StructField("eid",IntegerType(),True),
    StructField("ename",StringType(),True),
    StructField("did",IntegerType(),True),
    StructField("sal",IntegerType(),True),
    StructField("date",DateType(),True)
])

df=spark.read.format("csv").schema(header).option("inferschema","true").load("file:///Users/hemantvilasnawale/Desktop/wheader.csv")

df.show()