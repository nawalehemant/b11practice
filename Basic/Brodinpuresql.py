from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Brodcast Join Pure SQL").getOrCreate()


df_emp=spark.read.format("csv").option("header","true").option("delimitede",",").option("inferschema","true").\
    load("file:///C:/Users/Lenovo/Desktop/emp.csv")


df_dept=spark.read.format("csv").option("header","true").option("delimitede",",").option("inferschema","true").\
    load("file:///C:/Users/Lenovo/Desktop/DEPT.csv")

df_emp.show()
df_dept.show()


df_emp.createOrReplaceTempView("emp")
df_dept.createOrReplaceTempView("dept")

df_res=spark.sql("select  /*+ BROADCAST(dept) */ eid,ename,dname from emp inner join dept on emp.did=dept.did")

df_res.show()