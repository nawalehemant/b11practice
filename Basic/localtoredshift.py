from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("redshift").getOrCreate()

#read redshift

url="jdbc:redshift://redshift-cluster-1.cj04tz5ilvd0.ap-south-1.redshift.amazonaws.com:5439/dev"


df=spark.read.format("csv").option("header","true").option("inferaschema","true").load("d:/emp.csv")

df.show()

df.write.format("JDBC").option("url",url).option("user","awsuser").option("password","Awspassword1")\
    .option("dbtable","std").option("driver","com.amazon.redshift.Driver").save()

df.show()