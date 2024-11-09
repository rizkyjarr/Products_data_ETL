#IMPORTING LIBRARIES
import pandas as pd
import os
from etl_helper import extract_multi_csvs


# IMPORTING RAW DATA:
# Merging datasets into one dataframe. Function extract_multi_csvs can refer to etl_helper.py
directory_path = (r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\01. Personal Projects\01. ETL EXC 01\data_original")
files_list = os.listdir(directory_path)

df = extract_multi_csvs(directory_path,files_list)

# backup the original dataframe 
df1 = df.copy()

# DATA TRANSFORMATION AND CLEANING:
# Handling 'rating' columns, convert to float and fill NaN values with 0
df1['ratings'] = pd.to_numeric(df1['ratings'], errors='coerce')
df1['ratings'] = df1['ratings'].fillna(0)

# Handling 'no_of_ratings' column, same treat as 'rating' columns then convert into Integer
df1['no_of_ratings'] = pd.to_numeric(df1['no_of_ratings'].str.replace(",", ""),errors='coerce')
df1['no_of_ratings'] = df1['no_of_ratings'].fillna(0)
df1['no_of_ratings'] = df1['no_of_ratings'].astype('Int64')

# Handling 'discount_price' column, remove currency symbol, convert to Float, and fill NaN values with 0
df1['discount_price'] = pd.to_numeric(df1['discount_price'].str.replace("₹","").str.replace(",",""), errors='coerce')
df1['discount_price'] = df1['discount_price'].fillna(0)

# Handling 'actual_price' column, same treat as 'discount_price' column
df1['actual_price'] = pd.to_numeric(df1['actual_price'].str.replace("₹","").str.replace(",",""), errors='coerce')
df1['actual_price'] = df1['actual_price'].fillna(0)

# Drop data that has 0 in 'actual_price' column
df1 = df1[df1['actual_price'] != 0]

# Handling 'Unnamed: 0' column, removing this unnecessary column
df1 = df1.drop('Unnamed: 0', axis=1)

# Create new column named 'discount_amount' by using subtraction operator
df1['discount_amount'] = df1['actual_price'] - df1['discount_price']

# Drop duplicate rows within the dataframe
df_cleaned_v1 = df1.drop_duplicates()

# Drop duplicate row that has the same name and value but different in category
df_cleaned_v2 = df_cleaned_v1.groupby('name').first().reset_index()

# EXPORT DATAFRAME TO CSV FILE
export_path = (r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\01. Personal Projects\01. ETL EXC 01\data_cleaned\data_products_cleaned.csv")              
df_cleaned_v2.to_csv(export_path, index=False)