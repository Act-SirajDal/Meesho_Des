import pandas as pd
import sqlalchemy
import meesho_des.db_config as db
import os
from datetime import datetime


def file_generate():
    # Database connection setup
    db_url = f"mysql+pymysql://{db.db_user}:{db.db_password}@{db.db_host}/{db.db_name}"  # Replace placeholders with your database details
    engine = sqlalchemy.create_engine(db_url)

    # SQL table read
    table_name = f"{db.db_data_table}"  # Replace with your table name
    query = f"SELECT * FROM {table_name}"

    # Read table into DataFrame
    df = pd.read_sql(query, engine)

    # Drop the unwanted column
    if 'Product_Pin_PageSave_Status' in df.columns:
        df = df.drop(columns=['Product_Pin_PageSave_Status'])

    # Fill missing values with "N/A"
    df.fillna("N/A", inplace=True)


    # Write to Excel file
    output_file = f"C:\Siraj\Work\meesho_des\output\Meesho_Description_Data_20241206.xlsx"  # Name of the Excel file
    df.to_excel(output_file, index=False)

    print(f"Excel file generated: {output_file}")
