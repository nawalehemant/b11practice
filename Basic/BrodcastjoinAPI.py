from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Brodcast join API").getOrCreate()


df_emp=spark.read.format("csv").option("header","true").option("delimitede",",").option("inferschema","true").\
    load("file:///C:/Users/Lenovo/Desktop/emp.csv")


df_dept=spark.read.format("csv").option("header","true").option("delimitede",",").option("inferschema","true").\
    load("file:///C:/Users/Lenovo/Desktop/DEPT.csv")

df_emp.show()
df_dept.show()

df_res=df_emp.join(broadcast(df_dept),"did","inner").select("eid","ename","dname")

df_res.show()