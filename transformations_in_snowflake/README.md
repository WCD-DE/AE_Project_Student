These files contain all sql scripts to create data model and associated tables needed for the projects.
This assumes that you have downloaded all the data needed using Airbyte and Lambda function.
Brief description about these files and the order they should be used is given below.

1. data_exploration.sql -> Helps to understand TPCDS dataset.
2. data_modelling.sql -> Based on the requirements provided, helps to build the data model.
3. project_ddl.sql -> Create all the fact and dimension tables that you need to fulfill project requirements.
4. customer_dimension.sql -> Populates customer dimension based on project requirements.
5. daily_aggregated_sales.sql -> Creates sales fact with the sales aggregated to a day.
6. weekly_sales_inventory_fact.sql -> Creates sales fact with the sales aggregated to a week.
7. testing.sql -> Contains examples of tests that can be run on different tables.
8. tasks.sql -> Showcase how queries can be run on Snowflake on a schedule using tasks.