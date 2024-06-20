import os
import pandas as pd

# Print the current working directory
print("Current Working Directory:", os.getcwd())

# Ensure the file path is correct
#mutual_funds_file = '/Users/hemantvilasnawale/Documents/sharemarket/CSV/Fund_Table_final.csv'
investors_file = '/Users/hemantvilasnawale/Documents/sharemarket/CSV/Fund_Table_final.csv'

# Load the data
#mutual_funds = pd.read_csv(mutual_funds_file)
investors = pd.read_csv(investors_file)

# Display the first few rows of each DataFrame
#print(mutual_funds.head())
print(investors.head())

print("==========================================================================================================")

print("#########Display basic information about the dataframes##########")
# Display basic information about the dataframes
#print(mutual_funds.info())
print(investors.info())

print("##########Display summary statistics##########")
# Display summary statistics
#print(mutual_funds.describe())
print(investors.describe())

print("#############CHECK NULL###############")
# Verify changes
#print(mutual_funds.isnull().sum())
print(investors.isnull().sum())