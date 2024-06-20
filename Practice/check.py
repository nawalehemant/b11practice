from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


spark = SparkSession.builder.appName("Excel to PySpark").getOrCreate()

csv_file_path="/Users/hemantvilasnawale/Documents/sharemarket/CSV/Fund_Table_final"
# Read the CSV file into a DataFrame
df = spark.read.csv(f"file://{csv_file_path}", header=True, inferSchema=True)

# Show the DataFrame content
df.show()
df.printSchema()

print("duplicate check")
#duplicate check
df1=df.groupBy(df.F_ID).count().filter("count > 1")

df1.show()

print("null check")
#null check
df2=df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])

df2.show()

