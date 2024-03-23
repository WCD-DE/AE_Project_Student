-- Earliest and latest dates of the sales and inventory

----- Date Dim (Time Dimension)
select cal_dt, d_date_sk from TPCDS.RAW_AIR.DATE_DIM order by 1 limit 100; -- d_date_sk increases with cal_dt

----- Catalog Sales
select cal_dt from TPCDS.RAW_AIR.DATE_DIM where d_date_sk = (select min(CS_SOLD_DATE_SK) from TPCDS.RAW_AIR.CATALOG_SALES); -- 2021-01-01
select cal_dt from TPCDS.RAW_AIR.DATE_DIM where d_date_sk = (select max(CS_SOLD_DATE_SK) from TPCDS.RAW_AIR.CATALOG_SALES); -- 2023-10-14


----- Web Sales
select cal_dt from TPCDS.RAW_AIR.DATE_DIM where d_date_sk = (select min(WS_SOLD_DATE_SK) from TPCDS.RAW_AIR.WEB_SALES); -- 2021-01-02
select cal_dt from TPCDS.RAW_AIR.DATE_DIM where d_date_sk = (select max(WS_SOLD_DATE_SK) from TPCDS.RAW_AIR.WEB_SALES); -- 2023-10-14

------ Inventory
select cal_dt from TPCDS.RAW_AIR.DATE_DIM where d_date_sk = (select min(INV_DATE_SK) from TPCDS.RAW_AIR.INVENTORY); -- 2021-01-01
select cal_dt from TPCDS.RAW_AIR.DATE_DIM where d_date_sk = (select max(INV_DATE_SK) from TPCDS.RAW_AIR.INVENTORY); -- 2023-12-03


-- How frequent do we receive data in web_sales and inventory fact tables

----- Catalog Sales
select 
    d.cal_dt, 
    cs.CS_SOLD_DATE_SK,
    count(*) as count
from
    TPCDS.RAW_AIR.CATALOG_SALES cs
INNER JOIN TPCDS.RAW_AIR.DATE_DIM d
    ON cs.CS_SOLD_DATE_SK=d.d_date_sk
group by 1,2
order by 1 desc
limit 10;-- About 1000 records everyday

----- Web Sales
select 
    d.cal_dt, 
    ws.WS_SOLD_DATE_SK,
    count(*) as count
from
    TPCDS.RAW_AIR.WEB_SALES ws
INNER JOIN TPCDS.RAW_AIR.DATE_DIM d
    ON ws.WS_SOLD_DATE_SK=d.d_date_sk
group by 1,2
order by 1 desc
limit 10;-- About 500 records everyday

-- What information do we have available in tables and how do they connect to each other?

----- Catalog Sales
select * from TPCDS.RAW_AIR.CATALOG_SALES limit 100;

-- Verifying connection to date_dim using cs_sold_date_sk
select
    fact.cs_sold_date_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.DATE_DIM as dim
    ON dim.d_date_sk = fact.cs_sold_date_sk
limit 5;

-- Verifying connection to time_dim using cs_sold_time_sk
select
    fact.cs_sold_time_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.TIME_DIM as dim
    ON dim.t_time_sk = fact.cs_sold_time_sk
limit 5;

-- Verifying connection to date_dim using cs_ship_date_sk
select
    fact.cs_ship_date_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.DATE_DIM as dim
    ON dim.d_date_sk = fact.cs_ship_date_sk
limit 5;

-- Verifying connection to customer using cs_bill_customer_sk
select
    fact.cs_bill_customer_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.CUSTOMER as dim
    ON dim.c_customer_sk = fact.cs_bill_customer_sk
limit 5;

-- Verifying connection to customer demographics using cs_bill_cdemo_sk
select
    fact.cs_bill_cdemo_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.CUSTOMER_DEMOGRAPHICS as dim
    ON dim.cd_demo_sk = fact.cs_bill_cdemo_sk
limit 5;

-- Verifying connection to household demographics using cs_bill_hdemo_sk
select
    fact.cs_bill_hdemo_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.HOUSEHOLD_DEMOGRAPHICS as dim
    ON dim.hd_demo_sk = fact.cs_bill_hdemo_sk
limit 5;

-- Verifying connection to customer address
select
    fact.cs_bill_addr_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.CUSTOMER_ADDRESS as dim
    ON dim.ca_address_sk = fact.cs_bill_addr_sk
limit 5;

-- Verifying connection to customer using cs_ship_customer_sk
select
    fact.cs_ship_customer_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.CUSTOMER as dim
    ON dim.c_customer_sk = fact.cs_ship_customer_sk
limit 5;

-- Verifying connection to customer demographics using cs_ship_cdemo_sk
select
    fact.cs_ship_cdemo_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.CUSTOMER_DEMOGRAPHICS as dim
    ON dim.cd_demo_sk = fact.cs_bill_cdemo_sk
limit 5;

-- Verifying connection to household demographics using cs_ship_hdemo_sk
select
    fact.cs_ship_hdemo_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.HOUSEHOLD_DEMOGRAPHICS as dim
    ON dim.hd_demo_sk = fact.cs_ship_hdemo_sk
limit 5;

-- Verifying connection to call center using cs_call_center_sk
select
    fact.cs_call_center_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.CALL_CENTER as dim
    ON dim.cc_call_center_sk = fact.cs_call_center_sk
limit 5;

-- Empty columns CC_REC_END_DATE,CC_STREET_NAME, CC_CLOSED_DATE_SK
-- Verifying connection to catalog pages using cs_catalog_page_sk
select
    fact.cs_catalog_page_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.CATALOG_PAGE as dim
    ON dim.cp_catalog_page_sk = fact.cs_catalog_page_sk
limit 5;

-- Verifying connection to ship mode using cs_ship_mode_sk
select
    fact.cs_ship_mode_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.SHIP_MODE as dim
    ON dim.sm_ship_mode_sk = fact.cs_catalog_page_sk
limit 5;

-- Verifying connection to warehouse using cs_ship_mode_sk
select
    fact.cs_warehouse_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.WAREHOUSE as dim
    ON dim.w_warehouse_sk = fact.cs_warehouse_sk
limit 5;

-- Verifying connection to item using cs_item_sk
select
    fact.cs_item_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.ITEM as dim
    ON dim.i_item_sk = fact.cs_item_sk
limit 5;

-- Verifying connection to promo using cs_promo_sk
select
    fact.cs_promo_sk,
    dim.*
from 
    TPCDS.RAW_AIR.CATALOG_SALES as fact
INNER JOIN TPCDS.RAW_AIR.PROMOTION as dim
    ON dim.p_promo_sk = fact.cs_promo_sk
limit 5;

----- Web Sales
select * from TPCDS.RAW_AIR.WEB_SALES limit 100;

-- Verifying connection to web pages using ws_web_page_sk
select
    fact.ws_web_page_sk,
    dim.*
from 
    TPCDS.RAW_AIR.WEB_SALES as fact
INNER JOIN TPCDS.RAW_AIR.WEB_PAGE as dim
    ON dim.wp_web_page_sk = fact.ws_web_page_sk
limit 5;

-- Verifying connection to web site using ws_web_site_sk
select
    fact.ws_web_site_sk,
    dim.*
from 
    TPCDS.RAW_AIR.WEB_SALES as fact
INNER JOIN TPCDS.RAW_AIR.WEB_SITE as dim
    ON dim.web_site_sk = fact.ws_web_site_sk
limit 5;


-- To understand sales and inventory connection, pick up one item to know how frequently it is ordered and recorded in inventory

-- Pick up random item when we find repeatedly in inventory table
select 
    inv_date_sk, 
    inv_item_sk,
    count(*) as count
from
    TPCDS.RAW_AIR.INVENTORY
group by 1,2
order by 2,1;

-- For inv_item_sk 1, how frequently is it being recorded in the inventory table?
select
    DISTINCT
    date.cal_dt,
    date.wk_num
from 
    TPCDS.RAW_AIR.DATE_DIM as date
INNER JOIN TPCDS.RAW_AIR.INVENTORY as inv
    ON date.d_date_sk=inv.inv_date_sk
    AND inv_item_sk=1
ORDER BY 1,2; -- inventory table records it every week

-- For inv_item_sk 1, how frequently is it being ordered by the web sales customers
select
    DISTINCT
    date.cal_dt,
    date.wk_num
from 
  TPCDS.RAW_AIR.WEB_SALES as ws
INNER JOIN TPCDS.RAW_AIR.DATE_DIM as date
    ON date.d_date_sk = ws.ws_sold_date_sk
INNER JOIN TPCDS.RAW_AIR.ITEM as item
    ON item.i_item_sk=ws.ws_item_sk
    AND ws_item_sk=1
    -- i_item_desc = 'Powers will not get influences. Electoral ports should show low, annual chains. Now young visitors may pose now however final pages. Bitterly right children suit increasing, leading el'
ORDER BY 1,2; -- Erratic sales with every day as a lowest common factor

-- For inv_item_sk 1, how frequently is it being ordered by the catalog sales customers
select
    DISTINCT
    date.cal_dt,
    date.wk_num
from 
  TPCDS.RAW_AIR.CATALOG_SALES as cs
INNER JOIN TPCDS.RAW_AIR.DATE_DIM as date
    ON date.d_date_sk = cs.cs_sold_date_sk
INNER JOIN TPCDS.RAW_AIR.ITEM as item
    ON item.i_item_sk=cs.cs_item_sk
    AND cs_item_sk=1
ORDER BY 1,2; -- Erratic sales with every day as a lowest common factor

-- How many individual items are there? 
select
    DISTINCT
    i_item_sk,
    i_product_name
from
    TPCDS.RAW_AIR.ITEM
ORDER BY 1;

---- How many total items do we have
select count(distinct i_item_sk) from TPCDS.RAW_AIR.ITEM; -- 18000 items

-- How many individual customers are there?
select
    DISTINCT
    c_customer_sk,
    c_first_name,
    c_last_name
from
    TPCDS.RAW_AIR.CUSTOMER
ORDER BY 1;

---- What is the count of customers that we have?
select count(distinct c_customer_sk) from TPCDS.RAW_AIR.CUSTOMER; --100000 customers


-- Adding table descriptions
COMMENT ON TABLE TPCDS.RAW_AIR.DATE_DIM IS 'This table is used to convert d_date_sk key in other tables to calendar date. This is the date dimension in the source.';

COMMENT ON COLUMN TPCDS.RAW_AIR.DATE_DIM.CAL_DT IS 'Calendar Date';