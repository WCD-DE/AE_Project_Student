-- Tasks for Customer Dimensions

CREATE OR REPLACE TASK sf_tpcds.intermediate.merging_new_records_in_customer_snapshot
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON * 8 * * * UTC'
    AS
MERGE INTO SF_TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT t1
USING TPCDS.RAW_AIR.CUSTOMER t2
ON  t1.C_SALUTATION=t2.C_SALUTATION
    AND t1.C_PREFERRED_CUST_FLAG=t2.C_PREFERRED_CUST_FLAG 
    AND t1.C_FIRST_SALES_DATE_SK=t2.C_FIRST_SALES_DATE_SK 
    AND t1.C_CUSTOMER_SK=t2.C_CUSTOMER_SK
    AND t1.C_LOGIN=t2.C_LOGIN
    AND t1.C_CURRENT_CDEMO_SK=t2.C_CURRENT_CDEMO_SK
    AND t1.C_FIRST_NAME=t2.C_FIRST_NAME
    AND t1.C_CURRENT_HDEMO_SK=t2.C_CURRENT_HDEMO_SK
    AND t1.C_CURRENT_ADDR_SK=t2.C_CURRENT_ADDR_SK
    AND t1.C_LAST_NAME=t2.C_LAST_NAME
    AND t1.C_CUSTOMER_ID=t2.C_CUSTOMER_ID
    AND t1.C_LAST_REVIEW_DATE_SK = t2.C_LAST_REVIEW_DATE_SK
    AND t1.C_BIRTH_MONTH = t2.C_BIRTH_MONTH
    AND t1.C_BIRTH_COUNTRY = t2.C_BIRTH_COUNTRY
    AND t1.C_BIRTH_YEAR = t2.C_BIRTH_YEAR
    AND t1.C_BIRTH_DAY = t2.C_BIRTH_DAY
    AND t1.C_EMAIL_ADDRESS = t2.C_EMAIL_ADDRESS
    AND t1.C_FIRST_SHIPTO_DATE_SK = t2.C_FIRST_SHIPTO_DATE_SK
WHEN NOT MATCHED 
THEN INSERT (
    C_SALUTATION, 
    C_PREFERRED_CUST_FLAG, 
    C_FIRST_SALES_DATE_SK, 
    C_CUSTOMER_SK, C_LOGIN, 
    C_CURRENT_CDEMO_SK, 
    C_FIRST_NAME, 
    C_CURRENT_HDEMO_SK, 
    C_CURRENT_ADDR_SK, 
    C_LAST_NAME, 
    C_CUSTOMER_ID, 
    C_LAST_REVIEW_DATE_SK, 
    C_BIRTH_MONTH, 
    C_BIRTH_COUNTRY, 
    C_BIRTH_YEAR, 
    C_BIRTH_DAY, 
    C_EMAIL_ADDRESS, 
    C_FIRST_SHIPTO_DATE_SK,
    START_DATE,
    END_DATE)
VALUES (
    t2.C_SALUTATION, 
    t2.C_PREFERRED_CUST_FLAG, 
    t2.C_FIRST_SALES_DATE_SK, 
    t2.C_CUSTOMER_SK, 
    t2.C_LOGIN, 
    t2.C_CURRENT_CDEMO_SK, 
    t2.C_FIRST_NAME, 
    t2.C_CURRENT_HDEMO_SK, 
    t2.C_CURRENT_ADDR_SK, 
    t2.C_LAST_NAME, 
    t2.C_CUSTOMER_ID, 
    t2.C_LAST_REVIEW_DATE_SK, 
    t2.C_BIRTH_MONTH, 
    t2.C_BIRTH_COUNTRY, 
    t2.C_BIRTH_YEAR, 
    t2.C_BIRTH_DAY, 
    t2.C_EMAIL_ADDRESS, 
    t2.C_FIRST_SHIPTO_DATE_SK,
    CURRENT_DATE(),
    NULL
);

CREATE OR REPLACE TASK sf_tpcds.intermediate.updating_old_records_in_customer_snapshot
    WAREHOUSE = COMPUTE_WH
    AFTER merging_new_records_in_customer_snapshot
    AS
MERGE INTO SF_TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT t1
USING TPCDS.RAW_AIR.CUSTOMER t2
ON  t1.C_CUSTOMER_SK=t2.C_CUSTOMER_SK
WHEN MATCHED
    AND (
    t1.C_SALUTATION!=t2.C_SALUTATION
    OR t1.C_PREFERRED_CUST_FLAG!=t2.C_PREFERRED_CUST_FLAG 
    OR t1.C_FIRST_SALES_DATE_SK!=t2.C_FIRST_SALES_DATE_SK 
    OR t1.C_LOGIN!=t2.C_LOGIN
    OR t1.C_CURRENT_CDEMO_SK!=t2.C_CURRENT_CDEMO_SK
    OR t1.C_FIRST_NAME!=t2.C_FIRST_NAME
    OR t1.C_CURRENT_HDEMO_SK!=t2.C_CURRENT_HDEMO_SK
    OR t1.C_CURRENT_ADDR_SK!=t2.C_CURRENT_ADDR_SK
    OR t1.C_LAST_NAME!=t2.C_LAST_NAME
    OR t1.C_CUSTOMER_ID!=t2.C_CUSTOMER_ID
    OR t1.C_LAST_REVIEW_DATE_SK != t2.C_LAST_REVIEW_DATE_SK
    OR t1.C_BIRTH_MONTH != t2.C_BIRTH_MONTH
    OR t1.C_BIRTH_COUNTRY != t2.C_BIRTH_COUNTRY
    OR t1.C_BIRTH_YEAR != t2.C_BIRTH_YEAR
    OR t1.C_BIRTH_DAY != t2.C_BIRTH_DAY
    OR t1.C_EMAIL_ADDRESS != t2.C_EMAIL_ADDRESS
    OR t1.C_FIRST_SHIPTO_DATE_SK != t2.C_FIRST_SHIPTO_DATE_SK
    ) 
THEN UPDATE SET
    end_date = current_date();



CREATE OR REPLACE TASK sf_tpcds.intermediate.creating_customer_dimension
    WAREHOUSE = COMPUTE_WH
    AFTER updating_old_records_in_customer_snapshot
    AS
create or replace transient table SF_TPCDS.ANALYTICS.CUSTOMER_DIM as
        (select 
        C_SALUTATION,
        C_PREFERRED_CUST_FLAG,
        C_FIRST_SALES_DATE_SK,
        C_CUSTOMER_SK,
        C_LOGIN,
        C_CURRENT_CDEMO_SK,
        C_FIRST_NAME,
        C_CURRENT_HDEMO_SK,
        C_CURRENT_ADDR_SK,
        C_LAST_NAME,
        C_CUSTOMER_ID,
        C_LAST_REVIEW_DATE_SK,
        C_BIRTH_MONTH,
        C_BIRTH_COUNTRY,
        C_BIRTH_YEAR,
        C_BIRTH_DAY,
        C_EMAIL_ADDRESS,
        C_FIRST_SHIPTO_DATE_SK,
        CA_STREET_NAME,
        CA_SUITE_NUMBER,
        CA_STATE,
        CA_LOCATION_TYPE,
        CA_COUNTRY,
        CA_ADDRESS_ID,
        CA_COUNTY,
        CA_STREET_NUMBER,
        CA_ZIP,
        CA_CITY,
        CA_GMT_OFFSET,
        CD_DEP_EMPLOYED_COUNT,
        CD_DEP_COUNT,
        CD_CREDIT_RATING,
        CD_EDUCATION_STATUS,
        CD_PURCHASE_ESTIMATE,
        CD_MARITAL_STATUS,
        CD_DEP_COLLEGE_COUNT,
        CD_GENDER,
        HD_BUY_POTENTIAL,
        HD_DEP_COUNT,
        HD_VEHICLE_COUNT,
        HD_INCOME_BAND_SK,
        IB_LOWER_BOUND,
        IB_UPPER_BOUND,
        START_DATE,
        END_DATE
from SF_TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT
JOIN tpcds.raw_air.customer_address ON c_current_addr_sk = ca_address_sk
join tpcds.raw_air.customer_demographics ON c_current_cdemo_sk = cd_demo_sk
join tpcds.raw_air.household_demographics ON c_current_hdemo_sk = hd_demo_sk
join tpcds.raw_air.income_band ON HD_INCOME_BAND_SK = IB_INCOME_BAND_SK
        );

SHOW TASKS;
ALTER TASK creating_customer_dimension RESUME;
ALTER TASK updating_old_records_in_customer_snapshot RESUME;
ALTER TASK merging_new_records_in_customer_snapshot RESUME;


EXECUTE TASK merging_new_records_in_customer_snapshot;

-- Cleanup
ALTER TASK merging_new_records_in_customer_snapshot suspend;
drop task creating_customer_dimension;
drop task updating_old_records_in_customer_snapshot;
drop task merging_new_records_in_customer_snapshot;