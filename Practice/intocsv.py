import pandas as pd
from pyspark.sql import SparkSession

# Paths
excel_file_path = "/Users/hemantvilasnawale/Documents/sharemarket/Fund_Table_final.xlsx"
csv_file_path = "/Users/hemantvilasnawale/Documents/sharemarket/CSV/Fund_Table_final.csv', index=False"  # corrected to .csv

# Convert Excel to CSV
df_excel = pd.read_excel(excel_file_path, engine='openpyxl')  # specifying the engine
df_excel.to_csv(csv_file_path)

print(f"Excel file has been converted to CSV and saved at {csv_file_path}")

# Initialize Spark session
spark = SparkSession.builder.appName("Excel to PySpark").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv(f"file://{csv_file_path}", header=True, inferSchema=True)
#df.count()
# Show the DataFrame content
df.show()


#df1=df.groupBy(df.Inv_Id).count().filter("count > 1")

#df1.show()