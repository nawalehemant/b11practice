from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("FundTable").getOrCreate()

csv_file_path = "/Users/hemantvilasnawale/Documents/Fund_Table1.csv"

# Read the CSV file without header
df = spark.read.option("header", "false").csv(csv_file_path, inferSchema=True)

df.show()