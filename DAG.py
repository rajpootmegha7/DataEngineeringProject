from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import os
from google.cloud import storage


# Define the default_args dictionary to specify the DAG's default parameters
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    'covid_analysis_dag',
    default_args=default_args,
    description='DAG for COVID analysis with PySpark',
    schedule_interval='@daily',  # Adjust this based on your desired schedule
)
#Upload to gcs
def upload_to_gcs(local_file_path, gcs_bucket_name, gcs_remote_dir):
    """ Uploads a local file to a GCS bucket"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_remote_dir + file_path)
    blob.upload_from_filename(local_file_path)

# Define a PythonOperator to run the PySpark script
def run_pyspark_script():
    try:
        # Create a Spark session
        spark = SparkSession.builder.appName("ParquetReader").getOrCreate()

        # Continue with the rest of your script...
        # ...

    except Exception as e:
        print("Error:", str(e))
        # Print additional diagnostic information
        print("Check Spark installation, resource availability, port availability, and firewall settings.")
        # Print Spark configuration for additional insights
        print("Spark Configuration:")
        print(spark.sparkContext.getConf().getAll())

        # Stop the Spark session if it was partially created
        spark.stop()

    covid_data_path = "data/covid_dataset.parquet"
    hospital_data_path = "data/hospital_data/hospital_data.parquet"

    # Read the parquet files into the DataFrames
    hospital_df = spark.read.parquet(hospital_data_path)
    covid_df = spark.read.parquet(covid_data_path)

    # Continue with the rest of your script...
    hospital_df = spark.read.parquet(hospital_data_path)
    #hospital_df.show(10)
    joined_df = covid_df.join(hospital_df,'City')

    patient_count = covid_df.groupBy('City').agg(F.count('*').alias('p_count'))
    #patient_count.show(10)

    merged_df= joined_df.join(patient_count, 'City')
    #merged_df.show(10)
    poor_patient = joined_df.groupBy('City').agg(
    (F.sum('COVID_Beds_Available') - F.count('*')).alias('Poor_Patients')
    )
    covid_bed_citywise = merged_df.groupBy('City').agg(F.sum('COVID_Beds_Available').alias('Covid_beds_citywise'))
    new_df = covid_bed_citywise.join(merged_df, 'City', 'inner')
    #new_df.show(10)
    # Find the patient which didn't get the beds

    pune_df = new_df.filter(new_df.City == 'Pune')
    #pune_df.show(10)

    unique_df = pune_df.dropDuplicates(['Hospital_Name'])

    result_df = unique_df.withColumn( "patient_no_beds", F.col("Covid_beds_citywise")- F.col("p_count"))
    #Write this result dataframe into the csv file for backup
    result_df.write.csv('data/result_data/result.csv',header = True, mode= 'overwrite')
    
    #Store the resulted dataframe in the GCP bucket
    file_path = '/Users/meghasinghrajpoot/Documents/GitHub/DataEngineeringProject/data/result_data/result.csv/result.csv'

    df = pd.read_csv(file_path)
    print(df)

    credential_path = "msr-project-415908-4b94ba8da02e.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    
    
#Example usage
    local_csv_file = file_path
    gcs_bucket = 'msr_bucket'
    gcs_directory = 'dag/'

    upload_to_gcs(local_csv_file, gcs_bucket, gcs_directory)
        

    # Don't forget to stop the Spark session at the end
    spark.stop()

# Define the PythonOperator to run the PySpark script
run_pyspark_task = PythonOperator(
    task_id='run_pyspark_script',
    python_callable=run_pyspark_script,
    dag=dag,
)

# Define the dependencies
run_pyspark_task

if __name__ == "__main__":
    dag.cli()
