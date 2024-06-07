from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark=SparkSession.builder.appName("SAmple").getOrCreate()

#Load data from rds :

df=spark.read.format("JDBC").option("url","jdbc:mysql://db1.cvtejpto9s8c.ap-south-1.rds.amazonaws.com:3306/prod").\
    option("user","myuser").\
    option("password","mypassword").\
    option("dbtable","Employee").\
    option("driver","com.mysql.cj.jdbc.Driver").load()

df1=df.filter(df["city"]=="New York")

df1.show()

#writefile  : load data from rds and write into rds

df1.write.format("JDBC").option("url","jdbc:mysql://db1.cvtejpto9s8c.ap-south-1.rds.amazonaws.com:3306/prod").\
   option("user","myuser").\
option("password","mypassword").\
   option("dbtable","hemant").\
   option("driver","com.mysql.cj.jdbc.Driver").save()


#thise is for load data on hdfs from rds

df1.write.format("csv").save("/hemant1")