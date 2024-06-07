from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.builder.appName("CDCSCD").getOrCreate()

#oad inital data

df=spark.read.format("jdbc").option("url","jdbc:mysql://database-1.cb666m2g8vbj.ap-south-1.rds.amazonaws.com:3306/prod")\
.option("user","puser").option("password","ppassword").option("dbtable","cust").option("driver","com.mysql.cj.jdbc.Driver").load()

df1=df.withColumn("start_date",current_date()).withColumn("end_date",lit('')).withColumn("curr_flg",lit('Y'))

df1.write.format("CSV").option("header","true").save("D:/cust_dtl")

#read from tgt
dft=spark.read.format("CSV").option("header","true").option("inferschema","true").load("D:/cust_dtl1")

dft.show()

max_id=dft.agg(max("cid")).collect()[0][0]
max_rudate=dft.withColumn('rudate',when(dft['rudate']=='','1970-01-01').when(dft['rudate'].isNull(),'1970-01-01').otherwise(dft['rudate'])).agg(max("rudate")).collect()[0][0]

max_rudate=max_rudate[:11]

print(max_id)
print (max_rudate)

#updated and insered records
ins_query='select * from cust where cid > {}'.format(max_id)
upd_query='select * from cust where rudate is not null and rudate > {}'.format(max_rudate)


df_ins=spark.read.format("jdbc").option("url","jdbc:mysql://database-1.cb666m2g8vbj.ap-south-1.rds.amazonaws.com:3306/prod")\
.option("user","puser").option("password","ppassword").option("query",ins_query).option("driver","com.mysql.cj.jdbc.Driver").load()

df_upd=spark.read.format("jdbc").option("url","jdbc:mysql://database-1.cb666m2g8vbj.ap-south-1.rds.amazonaws.com:3306/prod")\
.option("user","puser").option("password","ppassword").option("query",upd_query).option("driver","com.mysql.cj.jdbc.Driver").load()


df_incre=df_ins.union(df_upd)

df_latest=df_incre.withColumn("start_date",current_date()).withColumn("end_date",lit('')).withColumn("curr_flg",lit('Y'))
##########################


dft_final=dft.withColumn("start_date",date_add(dft["start_date"],-2))

dfr1=dft_final.union(df_latest)

dfr2=dfr1.withColumn("rn",row_number().over(Window.partitionBy("cid").orderBy(desc("rudate"))))

dfr3=dfr2.withColumn("curr_flg",when(dfr2["rn"]>1,lit('N')).otherwise(dfr2["curr_flg"])).drop("rn")

dfr4=dfr3.withColumn("end_date",lead(date_add(dfr3["start_date"],-1)).over(Window.partitionBy("cid").orderBy("rudate")))

dfr4.show()

dfr4.write.format("CSV").mode('overwrite').option("header","true").save("D:/cust_dtl2")