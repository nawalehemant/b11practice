from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Sample").enableHiveSupport().getOrCreate()

df=spark.sql("select * from hemant")

df.show()

df.write.format("json").save("s3://allclass/data/hemant")

