from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Hbase to HDFS").getOrCreate()

df=spark.read.format("org.apache.phoenix.spark").option("header","true").option("zkurl","localhost:2181").option("table","emp_hemant").load()

df.show()

df1=df.withColumn("Time",current_timestamp())

df1.show()

df1.write.format("csv").option("header","true").option("mode","overwrite").save("/hemant1")

#spark-submit --jars /usr/lib/phoenix/phoenix-4.14.3-HBase-1.4-client.jar,/usr/lib/phoenix/phoenix-spark-4.14.3-HBase-1.4.jar --master yarn --driver-memory 1G --executor-memory 3G hemant.py