import pandas as pd
import numpy as np
from faker import Faker
import datetime

# This script will create three CSV files:
# 1. actuals.csv - A list of thousands of fake transactions
# 2. budget.csv - A high-level monthly budget 
# 3. drivers.csv - Daily operational stats 


# Initialize Faker, a library that creates fake data
fake = Faker()

# --- 1. Generate ACTUALS (Transactions) ---
print("Generating 'actuals.csv'...")
num_transactions = 20000
departments = ['Sales', 'Marketing', 'Engineering', 'Finance']
products = ['Software', 'Hardware', 'Services', 'Consulting']

actuals_data = []
for _ in range(num_transactions):
    actuals_data.append({
        'TRANSACTION_ID': fake.uuid4(),
        'TRANSACTION_DATE': fake.date_between(start_date='-1y', end_date='today'),
        'AMOUNT': np.random.randint(500, 50000),
        'DEPARTMENT': np.random.choice(departments),
        'PRODUCT_LINE': np.random.choice(products)
    })

df_actuals = pd.DataFrame(actuals_data)
df_actuals.to_csv('actuals.csv', index=False)


# --- 2. Generate BUDGET (Monthly Targets) ---
print("Generating 'budget.csv'...")
budget_data = []
# Create budget for last year and this year
for month in range(1, 13):
    for dept in departments:
        # Budget for last year (2024)
        budget_data.append({
            'BUDGET_MONTH': datetime.date(2024, month, 1),
            'DEPARTMENT': dept,
            'BUDGET_AMOUNT': np.random.randint(500000, 2000000)
        })
        # Budget for this year (2025)
        budget_data.append({
            'BUDGET_MONTH': datetime.date(2025, month, 1),
            'DEPARTMENT': dept,
            'BUDGET_AMOUNT': np.random.randint(600000, 2200000)
        })

df_budget = pd.DataFrame(budget_data)
df_budget.to_csv('budget.csv', index=False)


# --- 3. Generate DRIVERS (Daily Operational Data) ---
print("Generating 'drivers.csv'...")
drivers_data = []

# Get today's date and the date one year ago
today = datetime.date.today()
one_year_ago = today - datetime.timedelta(days=365)

for day in pd.date_range(start=one_year_ago, end=today):
    drivers_data.append({
        'METRIC_DATE': day.date(),
        'LEADS_GENERATED': np.random.randint(150, 500),
        'SALES_REPS_ACTIVE': np.random.randint(45, 55), # Simulates a stable sales team
        'WEBSITE_VISITORS': np.random.randint(10000, 25000)
    })

df_drivers = pd.DataFrame(drivers_data)
df_drivers.to_csv('drivers.csv', index=False)

print("All CSV files generated successfully!")