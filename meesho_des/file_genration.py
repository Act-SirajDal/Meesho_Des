import pandas as pd
import sqlalchemy
import db_config as db

# Database connection setup
db_url = f"mysql+pymysql://{db.db_user}:{db.db_password}@{db.db_host}/{db.db_name}"  # Replace placeholders with your database details
engine = sqlalchemy.create_engine(db_url)

# SQL table read
table_name = f"{db.db_data_table}"  # Replace with your table name
query = f"SELECT * FROM {table_name}"

# Read table into DataFrame
df = pd.read_sql(query, engine)
df = df.replace(r'^\s*$', None, regex=True)
df['Product_Sku_Name'] = df['Product_Sku_Name'].str.replace(r'\s+', ' ', regex=True).str.strip()

# Drop the unwanted column
if 'Product_Pin_PageSave_Status' in df.columns:
    df = df.drop(columns=['Product_Pin_PageSave_Status'])

# Fill missing values with "N/A"
df.fillna("N/A", inplace=True)

# Write to Excel file
output_file = "Meesho_Description_Data_20241217.xlsx"  # Name of the Excel file
df.to_excel(output_file, index=False)

print(f"Excel file generated: {output_file}")
