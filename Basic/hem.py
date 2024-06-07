from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark=SparkSession.builder.appName("SAmple").getOrCreate()

df=spark.read.format("JDBC").option("url","jdbc:mysql://aditya.cvtejpto9s8c.ap-south-1.rds.amazonaws.com:3306/prod").\
    option("user","myuser").\
    option("password","mypassword").\
    option("dbtable","employee").\
    option("driver","com.mysql.cj.jdbc.Driver").load()


df1=df.filter(df["city"]=="New York")

df1.show()

#writefile
df1.write.format("JDBC").option("url","jdbc:mysql://jdbc:mysql://aditya.cvtejpto9s8c.ap-south-1.rds.amazonaws.com:3306/prod").\
   option("user","myuser").\
   option("password","mypassword").\
   option("dbtable","hemant").\
   option("driver","com.mysql.cj.jdbc.Driver").save()

#df1.write.format("csv").save("/hemant1")