from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Initialize Spark session
spark = SparkSession.builder.appName("FundTable").getOrCreate()

# Path to your CSV file
csv_file_path = "/Users/hemantvilasnawale/Downloads/Fund_Table.csv"

# Load CSV file into a DataFrame
df = spark.read.csv(csv_file_path, header=True)

# Convert Invest_Per column to float type (assuming it's string)
df = df.withColumn("Invest_Per", df["Invest_Per"].cast("float"))

# Group by F_ID and sum Invest_Per
df_grouped = df.groupBy("F_ID").agg(sum("Invest_Per").alias("Total_Invest_Per"))

# Show the grouped DataFrame
df_grouped.show()

# Stop Spark session
spark.stop()
