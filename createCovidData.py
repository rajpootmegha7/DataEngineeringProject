import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Set random seed for reproducibility
np.random.seed(42)

# Function to generate synthetic data
def generate_covid_dataset(num_records, cities):
    data = {
        'Patient_ID': range(1, num_records + 1),
        'Health_Condition': ['Critical'] * num_records,
        'Vaccine_Status': ['Received'] * num_records,
        'Country': ['India'] * num_records,
        'City': np.random.choice(cities, num_records),
    }

    # Adding more columns if needed
    # ...

    # Create DataFrame
    df = pd.DataFrame(data)

    return df

# Set parameters
num_records = 200000  # 2 lakh records
cities = ['Pune', 'Hyderabad', 'Mumbai', 'Delhi', 'Bhubaneshwar', 'Goa', 'Bhopal', 'Noida', 'Indore', 'Jhansi', 'Chennai', 'Ooty']

# Generate synthetic dataset
covid_dataset = generate_covid_dataset(num_records, cities)

# Convert to PyArrow Table
table = pa.Table.from_pandas(covid_dataset)

# Write to Parquet file
output_path = 'covid_dataset.parquet'
pq.write_table(table, output_path)

print(f'Dataset with {num_records} records generated and saved to {output_path}.')
