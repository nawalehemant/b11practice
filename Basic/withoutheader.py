from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName("Without Header").getOrCreate()

header=StructType([
    StructField("eid",IntegerType(),True),
    StructField("ename",StringType(),True),
    StructField("did",IntegerType(),True),
    StructField("salary",IntegerType(),True),
    StructField("city",StringType(),True)
])

df=spark.read.format("csv").\
    schema(header).\
    option("inferschema","true").\
    load("file:///C:/Users/Lenovo/Desktop/emp.csv")

df.show()