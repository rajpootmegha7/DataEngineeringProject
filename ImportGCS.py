import pandas as pd
from google.cloud import storage #pip install google-cloud-storage

df = pd.read_csv('data/result_data/result.csv/result.csv')

print(df)

# Upload the data to cloud storage client 
client = storage.Client()
export_bucket = client.get_bucket('megha-gcp')
#df.to_csv()

export_bucket.blob('result {0}.csv').upload_from_string(df.to_csv(),'text/csv')

