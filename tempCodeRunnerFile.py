import pandas as pd
from google.cloud import storage #pip install google-cloud-storage

df = pd.read_csv('data/result_data/result.csv/result.csv')

print(df)