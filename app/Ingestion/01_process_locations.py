"""
This script is an AWS Glue job that processes location data from a CSV file stored in an S3 bucket. It performs the following steps:
1. Reads the `locations.csv` file from the specified S3 bucket and folder.
2. Renames the `load_ts` column to `load_timestamp` for consistency.
3. Adds metadata columns: `ingestion_timestamp` (current timestamp) and `file_name` (name of the source file).
4. Writes the transformed data back to a specified S3 bucket in CSV format.
"""


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue and Spark
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_BUCKET = "s3://hrms-oracle-265475006349/bronze/"
FOLDER = "locations"
file_name = "locations.csv"

# 1. Load the data
locations_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{SOURCE_BUCKET}{FOLDER}/{file_name}")

# 2. Rename columns
rename_map = {"load_ts": "load_timestamp"}
for old_name, new_name in rename_map.items():
    if old_name in locations_df.columns:
        locations_df = locations_df.withColumnRenamed(old_name, new_name)

# 3. Add metadata
locations_trans_df = locations_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                 .withColumn("file_name", lit(file_name))

output_path = "s3://hrms-oracle-265475006349/bronze/raw/locations/"
(
    locations_trans_df
    .write
    .format("csv")     # specify delta format
    .mode("overwrite")   # or "append"
    .save(output_path)
)

# Commit Glue job
job.commit()

# Stop Spark session
spark.stop()