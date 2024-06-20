from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Initialize Spark session
spark = SparkSession.builder.appName("FundTable").getOrCreate()

# Path to your CSV file
csv_file_path = "/Users/hemantvilasnawale/Documents/sharemarket/CSV/Fund_Table_final.csv"

# Read the CSV file with header, skipping the first row
df = spark.read.option("header", "false").csv(csv_file_path, inferSchema=True)

# Extract the second line (actual header)
new_header = df.head(2)[-1]

# Skip the first two lines and set the correct header
df = df.filter((df['_c0'] != new_header['_c0']) | (df['_c1'] != new_header['_c1'])).toDF(*[str(col) for col in new_header])

df.show()

# Convert Invest_Per column to float type (assuming it's string)
df = df.withColumn("Invest_Per (%)", df["Invest_Per (%)"].cast("float"))
df.printSchema()
# Group by F_ID and sum Invest_Per
df_grouped = df.groupBy("F_ID").agg(sum("Invest_Per (%)").alias("Total_Invest_Per"))

# Show the grouped DataFrame
df_grouped.show()

# Stop Spark session
spark.stop()
