import os
import random
from faker import Faker
from fastavro import writer, parse_schema

# Define Avro schema
avro_schema = {
    "type": "record",
    "name": "CovidPatient",
    "fields": [
        {"name": "patient_id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "city", "type": "string"},
        {"name": "health_condition", "type": "string"},
        {"name": "vaccination_status", "type": "boolean"},
    ],
}

fake = Faker("en_IN")  # Use Indian locale for fake data

def generate_dataset(num_records):
    dataset = []
    cities = ["Pune", "Hyderabad", "Mumbai", "Delhi", "Bhubaneshwar", "Goa", "Bhopal", "Noida", "Indore", "Jhansi"]

    for _ in range(num_records):
        patient_id = fake.uuid4()
        name = fake.name()
        age = random.randint(18, 80)
        city = random.choice(cities)
        health_condition = "Critical"
        vaccination_status = random.choice([True, False])

        record = {
            "patient_id": patient_id,
            "name": name,
            "age": age,
            "city": city,
            "health_condition": health_condition,
            "vaccination_status": vaccination_status,
        }

        dataset.append(record)

    return dataset

def write_avro_file(dataset, output_file):
    with open(output_file, "wb") as avro_file:
        writer(avro_file, parse_schema(avro_schema), dataset)

if __name__ == "__main__":
    output_file = "covid_patient_data.avro"
    num_records = 200000  # Minimum 2 lakh records

    if not os.path.exists(output_file):
        print(f"Generating {num_records} records and writing to {output_file}...")
        dataset = generate_dataset(num_records)
        write_avro_file(dataset, output_file)
        print("Dataset generation complete.")
    else:
        print("File already exists. Please remove or rename the existing file.")
