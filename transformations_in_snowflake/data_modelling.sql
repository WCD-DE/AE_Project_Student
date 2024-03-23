-- 1. Business Requirements
-- sum_qty_wk: sum(catalog_sales.cs_quantity) group by date_dim.week_num and item, OR sum(web_sales.ws_quantity) group by date_dim.week_num and item
    -- --> Grain: Week Number and Item
-- sum_amt_wk: sum(catalog_sales.cs_sales_price * catalog_sales.cs_quantity) group by date_dim.week_num, item OR sum(web_sales.ws_sales_price * web_sales.ws_quantity) group by date_dim.week_num, item
-- sum_profit_wk: sum(catalog_sales.cs_net_profit) group by date_dim.week_num, item OR sum(web_sales.ws_net_profit) group by date_dim.week_num, item
-- avg_qty_dy: = sum_qty_wk/7
-- inv_on_hand_qty_wk: inventory.inv_quantity_on_hand at date_dim.week_num, warehouse
-- wks_sply: = inv_on_hand_qty_wk/sum_qty_wk
-- low_stock_flg_wk: ((avg_qty_dy > 0 && ((avg_qty_dy) > (inventory_on_hand_qty_wk))
-- --> In order to understand inventory better, we should have warehouse as a grain as well
-- Integrate Customer Dimension: Customer(SCD Type 2) + Customer_Address + Customer_Demographics + Household_Demographics + Income_Band


-- 2. Data Model
-- Weekly Sales Inventory Fact Table
-- Dimensions: week_num, item, warehouse
-- Facts: sum_qty_wk, sum_amt_wk, sum_profit_wk, avg_qty_dy, inv_on_hand_qty_wk, wks_sply,low_stock_flg_wk

-- Customer Dimension
-- Customer
C_SALUTATION, C_PREFERRED_CUST_FLAG, C_FIRST_SALES_DATE_SK, C_CUSTOMER_SK, C_LOGIN, C_CURRENT_CDEMO_SK, C_FIRST_NAME, C_CURRENT_HDEMO_SK, C_CURRENT_ADDR_SK, C_LAST_NAME, C_CUSTOMER_ID, C_LAST_REVIEW_DATE_SK, C_BIRTH_MONTH, C_BIRTH_COUNTRY, C_BIRTH_YEAR, C_BIRTH_DAY, C_EMAIL_ADDRESS, C_FIRST_SHIPTO_DATE_SK

-- Customer Address
CA_STREET_NAME, CA_SUITE_NUMBER, CA_STATE, CA_LOCATION_TYPE, CA_ADDRESS_SK, CA_COUNTRY, CA_ADDRESS_ID, CA_COUNTY, CA_STREET_NUMBER, CA_ZIP, CA_CITY, CA_STREET_TYPE, CA_GMT_OFFSET

-- Customer Demographics
CD_DEP_EMPLOYED_COUNT, CD_DEMO_SK, CD_DEP_COUNT, CD_CREDIT_RATING, CD_EDUCATION_STATUS, CD_PURCHASE_ESTIMATE, CD_MARITAL_STATUS, CD_DEP_COLLEGE_COUNT, CD_GENDER

-- Household Demographics
HD_BUY_POTENTIAL, HD_INCOME_BAND_SK, HD_DEMO_SK, HD_DEP_COUNT, HD_VEHICLE_COUNT

-- Income Band
IB_LOWER_BOUND, IB_INCOME_BAND_SK, IB_UPPER_BOUND

-- SCD Type 2
Valid From, Valid To