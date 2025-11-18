# Snowflake Financial Planning & Analysis (FP&A) Project

This project establishes a robust, end to end data engineering and analysis pipeline for financial forecasting. The solution demonstrates proficiency in cloud data warehousing, data modeling, and predictive analytics.

## Solution Architecture

* ELT Process: An Extract Load Transform pipeline manages the ingestion of raw financial and operational data, utilizing efficient cloud storage and copy functionalities.
* Data Modeling: Raw data is structured and transformed using complex SQL views and tables, resulting in a clean analytics schema suitable for historical reporting and model training.
* Predictive Forecasting: A machine learning framework leverages the clean data to train a Linear Regression model. This model forecasts future revenue by analyzing key operational drivers (e.g., leads generated and active sales personnel).
* Security Management: All access credentials and sensitive connection parameters are secured using environment variables, ensuring strict separation from the codebase and preventing exposure in version control.

## Technology Stack

* Cloud Data Warehouse: Snowflake
* Modeling and Transformation: SQL
* Data Ingestion and Machine Learning: Python (Snowpark, scikit-learn, Pandas)
* Version Control: Git

## Deliverables

* Historical Performance View: A dynamic view providing real time comparison of budgeted versus actual financial performance.
* Revenue Forecast: A persistent table containing a 12 month, driver based revenue forecast for strategic planning.


