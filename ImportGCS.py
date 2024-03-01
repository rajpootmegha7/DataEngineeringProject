import pandas as pd
import os
from google.cloud import storage #pip install google-cloud-storage

file_path = '/Users/meghasinghrajpoot/Documents/GitHub/DataEngineeringProject/data/result_data/result.csv/result.csv'

df = pd.read_csv(file_path)
print(df)

credential_path = "msr-project-415908-4b94ba8da02e.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path


def upload_to_gcs(local_file_path, gcs_bucket_name, gcs_remote_dir):
    """ Uploads a local file to a GCS bucket"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_remote_dir + file_path)
    blob.upload_from_filename(local_file_path)
    
    
#Example usage
local_csv_file = file_path
gcs_bucket = 'msr_bucket'
gcs_directory = 'dag/'

upload_to_gcs(local_csv_file, gcs_bucket, gcs_directory)

