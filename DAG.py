from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    # ...

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
