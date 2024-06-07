from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("s3 to hbase").getOrCreate()

df=spark.read.format("org.apache.phoenix.spark").option("zkurl","localhost:2181").option("table","hd_hemant").load()

df.show()

df.write.format("csv").option("header","true").option("delimiter",",").option("inferschema","true").save("s3://rhushi98/hemant")

#spark-submit --jars /usr/lib/phoenix/phoenix-4.14.3-HBase-1.4-client.jar,/usr/lib/phoenix/phoenix-spark-4.14.3-HBase-1.4.jar --master yarn --driver-memory 1G --executor-memory 3G hemant.py