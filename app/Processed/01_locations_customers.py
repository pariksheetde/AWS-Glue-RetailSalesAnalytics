"""
This Glue job reads data from the `locations` and `customers` tables in the `hrms` database,
performs an inner join on the location_id, and writes the resulting DataFrame to S3 in Delta format.
The output includes the location_id, location_name, address, first_name, last_name and audit fields (loaded_by and load_timestamp).
To run this job, ensure that the necessary IAM permissions are in place for reading from the Glue Data Catalog and writing to S3.
The job can be executed in the AWS Glue console or via the AWS CLI, passing the required JOB_NAME parameter.
Example CLI command:
    aws glue start-job-run --job-name 01_locations_customers
Make sure to replace 'hrms-analytics-265475006349/processed/' with the appropriate S3 bucket and path where you want to store the output.
Note: This code assumes that the `locations` and `customers` tables have the specified schema and that the necessary AWS Glue libraries are available in the environment.
"""

import sys
import re
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, split, year, month, dayofmonth

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Configure SparkSession with Delta support
spark = (
    SparkSession.builder
        .appName("GlueDeltaJob")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

glue_database = "hrms"

# --- READ FROM GLUE DATA CATALOG ---
locations_df = glueContext.create_dynamic_frame.from_catalog(
    glue_database,
    table_name="locations",
    additional_options={'useCatalogSchema': True, 'useSparkDataSource': True}
).toDF()

customers_df = glueContext.create_dynamic_frame.from_catalog(
    glue_database,
    table_name="customers",
    additional_options={'useCatalogSchema': True, 'useSparkDataSource': True}
).toDF()

# Join condition
join_condition = locations_df.location_id == customers_df.locationid

# Perform join + select required columns
loc_cust_df = (
    locations_df.join(customers_df, join_condition, 'inner')
    .select(
        locations_df['location_id'],
        locations_df['location_name'],
        customers_df['address'],
        customers_df['name']
    )
    .withColumn('first_name', split(customers_df['name'], " ").getItem(0))
    .withColumn('last_name', split(customers_df['name'], " ").getItem(1))
    .withColumn('loaded_by', lit('pde1409'))
    .withColumn('load_timestamp', current_timestamp())
    .withColumn('year', year(current_timestamp()))
    .withColumn('month', month(current_timestamp()))
    .withColumn('day', dayofmonth(current_timestamp()))
    .drop(customers_df['name'])
)

loc_cust_df.show(truncate=False)

# --- WRITE TO S3 IN DELTA FORMAT ---
bucket_name = "hrms-analytics-265475006349"
output_prefix = "processed/locations_customers/"
output_path = f"s3://{bucket_name}/{output_prefix}"

(
    loc_cust_df
    .write
    .format("delta")   # ✅ Delta, not CSV
    .mode("append")    # append ensures new partitions are added
    .option("mergeSchema", "true")
    .partitionBy("year", "month", "day")
    .save(output_path)
)

# --- APPLY PART LOGIC TO PARQUET FILES ---
s3 = boto3.client("s3")

# Find existing suffix
objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=output_prefix)
max_suffix = 0
pattern = re.compile(r"locations_customers_(\d+)_part\d+\.parquet")

for obj in objects.get("Contents", []):
    key = obj["Key"]
    match = pattern.search(key)
    if match:
        suffix = int(match.group(1))
        max_suffix = max(max_suffix, suffix)

next_suffix = max_suffix + 1

# Rename each parquet part file inside partition folders
part_num = 1
for obj in objects.get("Contents", []):
    key = obj["Key"]
    # Only rename parquet part files, leave _delta_log untouched
    if key.endswith(".parquet") and "part" in key:
        partition_path = "/".join(key.split("/")[:-1])  # keep year/month/day path
        target_key = f"{partition_path}/locations_customers_{next_suffix:02d}_part{part_num}.parquet"
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": key},
            Key=target_key
        )
        s3.delete_object(Bucket=bucket_name, Key=key)
        part_num += 1

print(f"Renamed {part_num-1} parquet files with prefix locations_customers_{next_suffix:02d}_partN.parquet")

job.commit()

# STOP SPARK SESSION
spark.stop()