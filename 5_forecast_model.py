import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

# Import Snowpark libraries
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, month, year
from snowflake.snowpark.types import FloatType

import os
from dotenv import load_dotenv

load_dotenv() # This loads the variables from your .env file

# --- Load Snowflake Credentials from .env file ---
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
# ---

SNOWFLAKE_WAREHOUSE = "FPA_WAREHOUSE"
SNOWFLAKE_DATABASE = "ANALYTICS"
SNOWFLAKE_SCHEMA = "FINANCE" # We'll write our forecast here

def main():
    # --- 1. Connect to Snowflake ---
    # Create the connection parameters
    connection_parameters = {
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USER,
        "password": SNOWFLAKE_PASSWORD,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA
    }
    
    try:
        # Create the Snowpark session
        session = Session.builder.configs(connection_parameters).create()
        print("Snowpark session created successfully!")
    except Exception as e:
        print(f"Error creating Snowpark session: {e}")
        return

    # --- 2. Load and Prepare Data ---
    print("Loading data from Snowflake tables...")
    
    # Load our two "fact" tables from the ANALYTICS DB
    # We're loading the *total* company revenue, so we sum by month
    revenue_df = session.table("ANALYTICS.FINANCE.FACT_FINANCIALS_MONTHLY") \
                        .group_by(col("MONTH_START")) \
                        .sum(col("TOTAL_REVENUE")) \
                        .with_column_renamed("SUM(TOTAL_REVENUE)", "MONTHLY_REVENUE")

    # Load the monthly operations data
    ops_df = session.table("ANALYTICS.OPERATIONS.FACT_OPERATIONS_MONTHLY")

    # Join them together on the month
    # This creates our "model input" table
    model_input_df = revenue_df.join(ops_df, revenue_df.MONTH_START == ops_df.MONTH_START) \
                               .drop(ops_df.MONTH_START) # Drop the duplicate date col

    print("Successfully joined revenue and operations data.")
    
    # --- 3. Train the Model ---
    # Convert the Snowpark DataFrame to a pandas DataFrame to use scikit-learn
    print("Converting to pandas DataFrame for modeling...")
    pd_df = model_input_df.to_pandas()
    
    # Define our drivers (X) and our target (y)
    # This is the "driver-based" part
    X = pd_df[['AVG_MONTHLY_LEADS', 'AVG_MONTHLY_REPS', 'TOTAL_MONTHLY_VISITORS']]
    y = pd_df['MONTHLY_REVENUE']

    # Create and train the model
    print("Training Linear Regression model...")
    model = LinearRegression()
    model.fit(X, y)
    
    print("Model trained successfully.")

    # --- 4. Create a Future Forecast ---
    print("Generating future forecast...")
    
    # Create a pandas DataFrame with our "assumptions" for the next 12 months
    # This is where the 'what-if' analysis happens
    future_assumptions = {
        'AVG_MONTHLY_LEADS': [600, 610, 620, 630, 640, 650, 660, 670, 680, 690, 700, 710],
        'AVG_MONTHLY_REPS': [55, 55, 55, 56, 56, 56, 57, 57, 57, 58, 58, 58], # e.g., we hire a few reps
        'TOTAL_MONTHLY_VISITORS': [600000, 610000, 620000, 630000, 640000, 650000, 660000, 670000, 680000, 690000, 700000, 710000]
    }
    future_df = pd.DataFrame(future_assumptions)
    
    # Use our trained model to predict revenue based on these assumptions
    future_predictions = model.predict(future_df)
    future_df['FORECASTED_REVENUE'] = future_predictions
    
    # Add a date column for the forecast
    # We use pd.date_range to get the next 12 months
    future_df['FORECAST_MONTH'] = pd.date_range(start='2025-12-01', periods=12, freq='MS')

    print("Future forecast generated.")

    # --- 5. Save Forecast Back to Snowflake ---
    print("Saving forecast back to Snowflake...")
    
    # Convert our pandas forecast back to a Snowpark DataFrame
    snowpark_forecast_df = session.create_dataframe(future_df)
    
    # Save the data to a new table called "FORECAST_RESULTS"
    # 'overwrite' mode means it will replace the table every time we run this
    snowpark_forecast_df.write.mode("overwrite").save_as_table("ANALYTICS.FINANCE.FORECAST_RESULTS")
    
    print("Success! Forecast saved to ANALYTICS.FINANCE.FORECAST_RESULTS")
    session.close()

if __name__ == "__main__":
    main()