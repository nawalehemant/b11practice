from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
# Initialize Spark session
spark = SparkSession.builder.appName("Excel to PySpark").getOrCreate()


csv_file_path="/Users/hemantvilasnawale/Downloads/Transaction_Table (2).csv"
# Read the CSV file into a DataFrame
df = spark.read.csv(f"file://{csv_file_path}", header=True, inferSchema=True)
#df.count()
# Show the DataFrame content
df.show()

df1=df.filter(df["transaction_Type"]=="Purchase")
df1.show()

df2 = df1.groupBy("fund_id").agg(sum("Amt").alias("Total_Invest"))
#df2.write.format("csv").save("/Users/hemantvilasnawale/Downloads/Total_Invest.csv")
df2.show()