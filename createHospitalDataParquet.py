import pandas as pd
import random
import os

# Define cities and hospitals
cities = ["Pune", "Hyderabad", "Mumbai", "Delhi", "Bhubaneshwar", "Goa", "Bhopal", "Noida", "Indore", "Jhansi"]
num_cities = 20  # Number of cities to include in the dataset

# Generate data for hospitals
def generate_hospitals_data(city):
    num_hospitals = random.randint(1, 5)  # Random number of hospitals per city
    hospitals = []

    for _ in range(num_hospitals):
        hospital_name = f"{city} Hospital {random.randint(1, 100)}"
        beds_available = random.randint(50, 200)
        covid_beds_available = random.randint(10, beds_available)
        non_covid_beds_available = beds_available - covid_beds_available

        hospital_data = {
            "Hospital_Name": hospital_name,
            "Beds_Available": beds_available,
            "COVID_Beds_Available": covid_beds_available,
            "NonCOVID_Beds_Available": non_covid_beds_available,
        }

        hospitals.append(hospital_data)

    return hospitals

# Generate dataset
hospital_records = []
for city in cities:  # Remove random.sample line
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
