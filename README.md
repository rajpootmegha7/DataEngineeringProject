# DataEngineeringProject
1.Create an environment first in your local using following command for Mac:
python3 -m venv venv
source venv/bin/activate

2. pip install faker fastavro
3. pip install pandas
4. pip install pandas pyarrow


#This project contains 3 parts: 
Part1:
- Create a python script which will generate dataset for covid patient - who’s health condition was critical, who also got the covid vaccine injection , data is greater than 10MB and min 2 lack of record. File format will be Avro and country - india and for 10 cities.
- Create a dataset where each city has random no. of hospitals , take record of 20 cities and hospitals in Parque format it will contain details of how many beds were available for covid patients. 

Part 2:
- Now read the 2 different file formats in pyspark and create and transform the data , schema of no. of columns.
- Join 2 data frame 
- Now find out how many patients were left without beds in particular city for example in Pune. 
- Identify this information based on cities count.
- Save the output in GCP - write the data in GCS(create a free account in gcp)
After completing above tasks student will be comfortable with - how to create session, compute core and app name, sql transformation and store data into GCS, connectivity knowledge.

Part3:
- Create Airflow DAG - Add this script in DAG
- Patient data generated
- Hospital, geo location
- Spark job initiate
- processing 
- Store data GCS