from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CovidProject").getOrCreate()

patient_df = spark.read.format("avro").load(
    r"C:\Users\meghasinghrajpoot\Documents\GitHub\DataEngineeringProject\data\covid_patient_data.avro")

# patient_df.printSchema()
patient_df.show(10)