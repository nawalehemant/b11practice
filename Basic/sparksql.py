from pyspark.sql import SparkSession, DataFrame

spark=SparkSession.builder.appName("spark test").getOrCreate()

df_prod=spark.read.format("CSV").option("header","true").\
  option("delimiter",",").\
  option("inferschema","true").\
  load("file:////Users/hemantvilasnawale/Desktop/product.csv")

df_prod.show()



df_ord=spark.read.format("CSV").\
    option("delimiter",",").\
    option("header","true"). \
    option("inferschema", "true"). \
    load("file:///Users/hemantvilasnawale/Desktop/order.csv")

df_ord.show()

#create view

df_prod.createOrReplaceTempView("product")
df_ord.createOrReplaceTempView("order")


df_res=spark.sql("select * from product inner join order on product.ProductID=order.ProductID")
df_res.show()