-- Set context
USE DATABASE ANALYTICS;
USE SCHEMA FINANCE;
USE WAREHOUSE FP_A_WAREHOUSE;

-- ----------------------------------------------------------------------------
-- STEP 1: DIM_CALENDAR
-- ----------------------------------------------------------------------------
-- Warehouse best practice. Makes time-based queries easy.
CREATE OR REPLACE TABLE ANALYTICS.FINANCE.DIM_CALENDAR AS
SELECT
    DATEADD(day, seq4(), '2023-01-01')::DATE AS CALENDAR_DATE,
    YEAR(CALENDAR_DATE) AS CALENDAR_YEAR,
    MONTH(CALENDAR_DATE) AS MONTH_NUMBER,
    MONTHNAME(CALENDAR_DATE) AS MONTH_NAME,
    QUARTER(CALENDAR_DATE) AS QUARTER,
    DAYOFWEEK(CALENDAR_DATE) AS DAY_OF_WEEK,
    WEEKOFYEAR(CALENDAR_DATE) AS WEEK_OF_YEAR,
    DATE_TRUNC('month', CALENDAR_DATE)::DATE AS FIRST_DAY_OF_MONTH
FROM
    TABLE(GENERATOR(ROWCOUNT => (365 * 5))) -- 5 years of dates
ORDER BY
    CALENDAR_DATE;



-- STEP 2: FACT TABLES (MONTHLY ROLLUPS)

-- Aggregate raw data into monthly summaries for performance.

-- Roll up raw transactions to get monthly revenue
CREATE OR REPLACE TABLE ANALYTICS.FINANCE.FACT_FINANCIALS_MONTHLY AS
SELECT
    DATE_TRUNC('month', TRANSACTION_DATE)::DATE AS MONTH_START, -- Group by month
    DEPARTMENT,
    SUM(AMOUNT) AS TOTAL_REVENUE
FROM
    RAW_DATA.STAGING.RAW_TRANSACTIONS
GROUP BY
    MONTH_START, DEPARTMENT;


-- Roll up daily operational drivers to monthly
CREATE OR REPLACE TABLE ANALYTICS.OPERATIONS.FACT_OPERATIONS_MONTHLY AS
SELECT
    DATE_TRUNC('month', METRIC_DATE)::DATE AS MONTH_START,
    AVG(LEADS_GENERATED) AS AVG_MONTHLY_LEADS,
    AVG(SALES_REPS_ACTIVE) AS AVG_MONTHLY_REPS,
    SUM(WEBSITE_VISITORS) AS TOTAL_MONTHLY_VISITORS
FROM
    RAW_DATA.STAGING.RAW_DRIVERS
GROUP BY
    MONTH_START;


-- ----------------------------------------------------------------------------
-- STEP 3: BUDGET VS. ACTUALS (BvA) VIEW
-- ----------------------------------------------------------------------------
-- Final BvA report.
-- Built as a VIEW so it's always up to date.

CREATE OR REPLACE VIEW ANALYTICS.FINANCE.V_BUDGET_VS_ACTUALS AS
SELECT
    cal.MONTH_NAME,
    cal.CALENDAR_YEAR,
    bud.DEPARTMENT,
    bud.BUDGET_AMOUNT,
    
    -- Handle NULLs for months with no actuals
    IFF(act.TOTAL_REVENUE IS NULL, 0, act.TOTAL_REVENUE) AS ACTUAL_REVENUE,
    
    -- Calculate variance
    (ACTUAL_REVENUE - bud.BUDGET_AMOUNT) AS VARIANCE,
    (VARIANCE / bud.BUDGET_AMOUNT) * 100 AS VARIANCE_PERCENTAGE
FROM
    RAW_DATA.STAGING.RAW_BUDGET AS bud
    
-- Join budget to our new monthly actuals
LEFT JOIN ANALYTICS.FINANCE.FACT_FINANCIALS_MONTHLY AS act
    ON bud.BUDGET_MONTH = act.MONTH_START
    AND bud.DEPARTMENT = act.DEPARTMENT

-- Join to calendar for clean date names
LEFT JOIN ANALYTICS.FINANCE.DIM_CALENDAR AS cal
    ON bud.BUDGET_MONTH = cal.CALENDAR_DATE
WHERE
    -- Only show past/current data
    bud.BUDGET_MONTH <= CURRENT_DATE()
ORDER BY
    cal.CALENDAR_YEAR, cal.MONTH_NUMBER, bud.DEPARTMENT;