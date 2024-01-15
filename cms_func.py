import requests
import boto3
from datetime import datetime
from datetime import timedelta 
import pandas as pd
import os
from contextlib import closing


def download_csv(url, file_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, 'wb') as file:
            file.write(response.content)
    else:
        raise Exception("Failed to download file")

def transform_pt_exp():

    dfs = pd.read_csv('/tmp/grp_public_reporting_cahps.csv')

    # Dictionary mapping old names to new: {'old_name': 'new_name'}
    rename_dict = {' org_PAC_ID': 'org_pac_id', ' measure_cd': 'measure_cd', ' measure_title': 'measure_title', ' prf_rate': 'prf_rate', ' patient_count': 'patient_count', ' FN': 'FN'}

    # Rename the columns
    dfs.rename(columns=rename_dict, inplace=True)

    dfs = dfs[dfs['FN'] != 1.0]

    dfs.drop('FN', axis=1, inplace=True)

    # Save the transformed CSV file
    dfs.to_csv('/tmp/grp_public_reporting_cahps.csv', sep='|', index=False)

def transform_dac():

    chunk_size = 100  # Adjust this size to your needs
    chunks = pd.read_csv('/tmp/dac_national.csv', chunksize=chunk_size)

    for dfl in chunks:

        dfl = pd.read_csv('/tmp/dac_national.csv')

        # Let's drop out some columns:

        # line 2 supprression -> The marker of line 2 suppression means that there were multiple possible addresses for that clinician in the same building. If
        # users need to find a mailing address for any entries with this flag, users may want to search for additional information like a suite number

        columns_to_drop = ['sec_spec_1', 'sec_spec_2', 'sec_spec_3', 'sec_spec_4', 'Ind_PAC_ID', 'Provider Middle Name', 'suff', 'Telephone Number', 'adr_ln_2', 'ln_2_sprs', 'Facility Name']  # Replace with the names of the columns you want to drop

        # Drop the columns in place
        dfl.drop(columns=columns_to_drop, inplace=True)

        # drop NPI of 1225670078 whose ln_2_sprs is both Y and Null

        mask = dfl['NPI'] != 1225670078

        # Apply the mask to filter the DataFrame
        dfl = dfl[mask]

        # drop duplicates 
        dfl = dfl.drop_duplicates()

        # Calculate years since graduation
        current_year = 2021
        dfl['Years_exp'] = current_year - dfl['Grd_yr']

        # Correct negative values (future graduation years)
        dfl['Years_exp'] = dfl['Years_exp'].apply(lambda x: max(x, 0))

        # Transformation the zipcode column
        def transform_zipcode(zipcode):
            zipcode = str(zipcode)
            if len(zipcode) in [9, 8, 5]:
                return zipcode[:5]  # Truncate to 5 digits
            else:
                return 'Unk'    # Categorize as 'Unknown'

        # Apply the transformation
        dfl['zipcode'] = dfl['ZIP Code'].apply(transform_zipcode)

        # drop 'ZIP Code'
        dfl.drop(dfl.columns[16], axis=1, inplace=True)

        # engineer the value of Telehealth to be either 0 or 1 
        dfl['Telehlth'] = dfl['Telehlth'].fillna(0)
        dfl['Telehlth'] = dfl['Telehlth'].replace('Y', 1)

        # Save the transformed CSV file
        dfl.to_csv('/tmp/dac_national_transformed.csv', sep='|', index=False, mode='a', header=not os.path.exists('/tmp/dac_national_transformed.csv'))

    # dfl.to_csv('/tmp/dac_national.csv', sep='|', index=False)


def upload_to_s3(upload_name, bucket_name, name_on_s3):
    s3 = boto3.client('s3')
    s3.upload_file(upload_name, bucket_name, name_on_s3)



def download_large_csv(url, file_path):
    with closing(requests.get(url, stream=True)) as response:
        if response.status_code == 200:
            with open(file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                    file.write(chunk)
                    # Log progress here if needed
        else:
            # Log an error message here
            raise Exception(f"Failed to download file with status code: {response.status_code}")

def cleanup_temp_files(file_path):
    try:
        os.remove(file_path)
    except OSError as e:
        print(f"Error: {file_path} : {e.strerror}")
