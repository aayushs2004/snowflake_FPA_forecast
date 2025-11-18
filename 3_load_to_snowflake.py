import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv

load_dotenv() # This loads the variables from your .env file

# --- Load Snowflake Credentials from .env file ---
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
# ---

SNOWFLAKE_WAREHOUSE = "FPA_WAREHOUSE"
SNOWFLAKE_DATABASE = "RAW_DATA"
SNOWFLAKE_SCHEMA = "STAGING"

def upload_to_snowflake(csv_file, table_name, conn):
    print(f"Reading '{csv_file}'...")

    df = pd.read_csv(csv_file)              
    df.columns = df.columns.str.upper()     

    write_pandas(
        conn,
        df,
        table_name,
        auto_create_table=False,
        overwrite=True
    )

    print(f"Successfully uploaded {len(df)} rows to {table_name}.")
    return len(df)

def main():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print("Connected to Snowflake successfully!")
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return

    files_to_upload = [
       ('actuals.csv', 'RAW_TRANSACTIONS'),
        ('budget.csv', 'RAW_BUDGET'),
        ('drivers.csv', 'RAW_DRIVERS')
    ]

    total_rows = 0
    for csv_file, table_name in files_to_upload:
        total_rows += upload_to_snowflake(csv_file, table_name, conn)

    conn.close()
    print(f"All uploads complete. {total_rows} total rows loaded. Connection closed.")

if __name__ == "__main__":
    main()