import pandas as pd

# Read the Excel file
df = pd.read_excel('/Users/hemantvilasnawale/Downloads/Transaction_Table (1).xlsx')

# Write the DataFrame to a CSV file
df.to_csv('/Users/hemantvilasnawale/Downloads/Transaction_Table (2).csv')
