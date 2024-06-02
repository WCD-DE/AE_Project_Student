import os
import boto3
import requests
import snowflake.connector as sf


def lambda_handler(event, context):

    url = 'https://de-materials-tpcds.s3.ca-central-1.amazonaws.com/inventory.csv'
    destination_folder = '/tmp'
    file_name = 'inventory.csv'
    local_file_path = '/tmp/inventory.csv'
    
    # Snowflake connection parameters
    account = 'lcryxrf-kj08803'
    warehouse = 'COMPUTE_WH'
    database = 'tpcds'
    schema = 'raw_air'
    table = 'inventory'
    user = 'wcdsnow'
    password = 'Wcd123456'
    role='accountadmin'
    stage_name = 'inv_Stage'
    
    # Your AWS Access Key and Secret
    aws_access_key_id = "put your aws access key here"
    aws_secret_access_key = "put your aws secret access key here"
    

    client = boto3.client(
        's3',    
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
        )

    # use boto3 to download the file from the S3 Requester Pays bucket
    client.download_file(Bucket='de-materials-tpcds', Key=file_name, Filename=local_file_path, ExtraArgs={'RequestPayer': 'requester'})

    # Print the file content
    with open(local_file_path, 'r') as file:
        file_content = file.read()
        print("File Content:")
        print(file_content)
        



    # Establish Snowflake connection
    conn = sf.connect(user = user, password = password, \
                 account = account, warehouse=warehouse, \
                  database=database,  schema=schema,  role=role)


    cursor = conn.cursor()
    
    # use schema
    use_schema = f"use schema {schema};"
    cursor.execute(use_schema)
    
    # create CSV format
    create_csv_format = f"CREATE or REPLACE FILE FORMAT COMMA_CSV TYPE ='CSV' FIELD_DELIMITER = ',';"
    cursor.execute(create_csv_format)
    

    
    create_stage_query = f"CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT =COMMA_CSV"
    cursor.execute(create_stage_query)

    # Copy the file from local to the stage
    copy_into_stage_query = f"PUT 'file://{local_file_path}' @{stage_name}"
    cursor.execute(copy_into_stage_query)
    
    # List the stage
    list_stage_query = f"LIST @{stage_name}"
    cursor.execute(list_stage_query)
    
    # truncate table
    truncate_table = f"truncate table {schema}.{table};"  
    cursor.execute(truncate_table)    


    # Load the data from the stage into a table (example)
    copy_into_query = f"COPY INTO {schema}.{table} FROM @{stage_name}/{file_name} FILE_FORMAT =COMMA_CSV;"  
    cursor.execute(copy_into_query)


    print("File uploaded to Snowflake successfully.")


    return {
        'statusCode': 200,
        'body': 'File downloaded and uploaded to Snowflake successfully.'
    }
