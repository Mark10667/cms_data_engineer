from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import boto3
from datetime import datetime
from datetime import timedelta 
import pandas as pd
from contextlib import closing

from cmd_func import download_csv
from cmd_func import transform_pt_exp
from cmd_func import transform_dac
from cmd_func import upload_to_s3
from cmd_func import cleanup_temp_files
from cmd_func import download_large_csv


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False
    # other default args
}

dag = DAG('download_and_upload_csv_2', default_args=default_args, schedule_interval='@monthly')


# # task for patient experience file

download_task_pt_exp = PythonOperator(
    task_id='download_pt_exp_csv',
    python_callable=download_csv,
    op_kwargs={'url': "https://data.cms.gov/provider-data/sites/default/files/resources/6083bb351293e72eda42881d6a93d803_1692662721/grp_public_reporting_cahps.csv",
              'file_path': '/tmp/grp_public_reporting_cahps.csv'},
    dag=dag)

transform_task_pt_exp = PythonOperator(
    task_id='transform_pt_exp_csv',
    python_callable=transform_pt_exp,
    dag=dag)

upload_task_pt_exp = PythonOperator(
    task_id='upload_pt_exp_to_s3',
    python_callable=upload_to_s3,
    op_kwargs={'upload_name': '/tmp/grp_public_reporting_cahps.csv', 
    'bucket_name': 'cms-airflow-bucket', 
    'name_on_s3': 'grp_cahps.csv'},
    dag=dag)
    
cleanup_pt_exp = PythonOperator(
    task_id='cleanup_temp_pt_exp',
    python_callable=cleanup_temp_files,
    op_kwargs={'file_path': '/tmp/grp_public_reporting_cahps.csv'},
    dag=dag)

download_task_pt_exp >> transform_task_pt_exp >> upload_task_pt_exp >> cleanup_pt_exp

# # task for the dac national file 

# download_task_dac = PythonOperator(
#     task_id='download_dac_csv',
#     python_callable=download_large_csv,
#     op_kwargs={'url': "https://data.cms.gov/provider-data/sites/default/files/resources/69a75aa9d3dc1aed6b881725cf0ddc12_1703261120/DAC_NationalDownloadableFile.csv",
#               'file_path': '/tmp/dac_national.csv'},
#     dag=dag)

# transform_task_dac = PythonOperator(
#     task_id='transform_dac_csv',
#     python_callable=transform_dac,
#     dag=dag)

# upload_task_dac = PythonOperator(
#     task_id='upload_dac_to_s3',
#     python_callable=upload_to_s3,
#     op_kwargs={'upload_name': '/tmp/dac_national.csv', 
#     'bucket_name': 'cms-airflow-bucket', 
#     'name_on_s3': 'dac_national.csv'},
#     dag=dag)
    
# cleanup_dac = PythonOperator(
#     task_id='cleanup_temp_dac',
#     python_callable=cleanup_temp_files,
#     op_kwargs={'file_path': '/tmp/dac_national.csv'},
#     dag=dag)

# download_task_dac >> transform_task_dac >> upload_task_dac >> cleanup_dac


## MIPS Measurement 

download_task_mips = PythonOperator(
    task_id='download_mips_csv',
    python_callable=download_csv,
    op_kwargs={'url': "https://data.cms.gov/provider-data/sites/default/files/resources/a0f235e13d54670824f07977299e80e3_1697774725/ec_score_file.csv",
              'file_path': '/tmp/mips_raw.csv'},
    dag=dag)

upload_task_mips = PythonOperator(
    task_id='upload_mips_to_s3',
    python_callable=upload_to_s3,
    op_kwargs={'upload_name': '/tmp/mips_raw.csv', 
    'bucket_name': 'cms-airflow-bucket-raw', 
    'name_on_s3': 'mips_raw.csv'},
    dag=dag)
    
cleanup_mips = PythonOperator(
    task_id='cleanup_temp_mips',
    python_callable=cleanup_temp_files,
    op_kwargs={'file_path': '/tmp/mips_raw.csv'},
    dag=dag)

download_task_mips >> upload_task_mips >> cleanup_mips

# code below try to download the file directly to s3 bucket
# s3 = boto3.client('s3')

# def download_file_to_s3(url, bucket_name, object_name):
#     with closing(requests.get(url, stream=True)) as response:
#         if response.status_code == 200:
#             s3.upload_fileobj(response.raw, bucket_name, object_name)
#         else:
#             raise Exception(f"Failed to download file with status code: {response.status_code}")
        

# upload_raw_to_s3 = PythonOperator(
#     task_id='upload_raw_to_s3',
#     python_callable=download_file_to_s3,
#     op_kwargs={'url': "https://data.cms.gov/provider-data/sites/default/files/resources/69a75aa9d3dc1aed6b881725cf0ddc12_1703261120/DAC_NationalDownloadableFile.csv",
#               'bucket_name': 'cms-airflow-bucket-raw',
#               'object_name': 'dac_national_raw.csv'},
#     dag=dag)


