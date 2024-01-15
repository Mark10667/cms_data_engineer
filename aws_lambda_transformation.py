import json
import boto3
import pandas as pd
from io import BytesIO, StringIO
import gzip


s3_client = boto3.client('s3')

    
def tranform_mips(df_mips):

    columns_to_drop = [' Facility-based scoring Certification number', ' Facility Name', ' Cost_category_score', ' Provider Last Name',  ' Provider First Name']  

    # Drop the columns in place
    df_mips.drop(columns=columns_to_drop, inplace=True)

    # Dictionary mapping old names to new: {'old_name': 'new_name'}
    rename_dict = {' Org_PAC_ID': 'org_pac_id', ' source': 'source', ' Quality_category_score': 'Quality_category_score', ' PI_category_score': 'PI_category_score', ' IA_category_score': 'IA_category_score', ' final_MIPS_score_without_CPB': 'final_MIPS_score_without_CPB', ' final_MIPS_score': 'final_MIPS_score'}

    # Rename the columns
    df_mips.rename(columns=rename_dict, inplace=True)

    df_mips = df_mips.dropna(subset=['org_pac_id'])

    return df_mips


def lambda_handler(event, context):
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    target_bucket = 'cms-airflow-bucket'  # Replace with your target bucket name
    
    print("source_bucket is", source_bucket)
    print("object_key is", object_key)

    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    print(response)
    
    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket=source_bucket, Key=object_key)
    
    # Get object from S3
    csv_file = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    csv_body = csv_file['Body'].read()
    

    # Check if the file is gzip compressed and decompress if necessary
    if object_key.endswith('.gz'):
        gzipped_file = BytesIO(csv_body)
        with gzip.open(gzipped_file, 'rt', encoding='utf-8') as f:
            df = pd.read_csv(f)
    else:
        df = pd.read_csv(BytesIO(csv_body))


    # Transform the DataFrame
    df_transformed = tranform_mips(df)

    # Convert DataFrame to CSV format
    csv_data = df_transformed.to_csv(index=False, sep='|')
    
    # Upload CSV to S3
    bucket_name = target_bucket
    target_object_key = "mips_clean.csv"
    s3_client.put_object(Bucket=bucket_name, Key=target_object_key, Body=csv_data)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
