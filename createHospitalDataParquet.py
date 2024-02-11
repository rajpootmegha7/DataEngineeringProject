import pandas as pd
import numpy as np
import random
import os
import uuid
# Define cities and hospitals
cities = ["Pune", "Hyderabad", "Mumbai", "Delhi", "Bhubaneshwar", "Goa", "Bhopal", "Noida", "Indore", "Jhansi"]
num_records = 200000

# Generate unique string
def unique_string(city):
    unique_string = city + str(uuid.uuid4())
    return (unique_string)

# Generate data for hospitals
def generate_hospitals_data(city):
    num_hospitals = random.randint(1, 5)  # Random number of hospitals per city
    hospitals = []

    for _ in range(num_hospitals):
        
        hospital_name = unique_string(city)
        beds_available = random.randint(50, 200)
        covid_beds_available = random.randint(10, beds_available)
        non_covid_beds_available = beds_available - covid_beds_available

        hospital_data = {
            "Hospital_Name": hospital_name,
            "City": city,
            "Beds_Available": beds_available,
            "COVID_Beds_Available": covid_beds_available,
            "NonCOVID_Beds_Available": non_covid_beds_available,
        }

        hospitals.append(hospital_data)

    return hospitals

# Generate dataset
hospital_records = []
for city in cities:
    hospitals_data = generate_hospitals_data(city)
    hospital_records.extend(hospitals_data)

# Convert to DataFrame
df = pd.DataFrame(hospital_records)

# Write to Parquet file
output_dir = "hospital_data.parquet"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "hospital_data.parquet")

df.to_parquet(output_file, index=False)

print(f"Dataset generated and saved to {output_file}")
