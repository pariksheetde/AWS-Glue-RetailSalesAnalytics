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
from awsglue.transforms import *
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

# --- READ FROM GLUE DATA CATALOG ---
locations_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="hrms",
    table_name="locations"
)

customers_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="hrms",
    table_name="customers"
)

# Convert to DataFrames
locations_df = locations_dyf.toDF()
customers_df = customers_dyf.toDF()

# Print schemas
locations_df.printSchema()
customers_df.printSchema()

# Join condition
join_condition = locations_df.location_id == customers_df.locationid

# Perform join
location_customers_df = locations_df.join(customers_df, join_condition, 'inner')

# Select required columns + audit fields
loc_cust_df = (
    location_customers_df.select(
        locations_df['location_id'],
        locations_df['location_name'],
        customers_df['address'],
        customers_df['name']
    )
    .withColumn('first_name', split(customers_df['name'], " ").getItem(0))
    .withColumn('last_name', split(customers_df['name'], " ").getItem(1))
    .withColumn('loaded_by', lit('pde1409'))
    .withColumn('load_timestamp', current_timestamp())
    # derive partition columns
    .withColumn('year', year(current_timestamp()))
    .withColumn('month', month(current_timestamp()))
    .withColumn('day', dayofmonth(current_timestamp()))
).drop(customers_df['name'])

loc_cust_df.show(truncate=False)

# --- WRITE TO S3 IN DELTA FORMAT ---
output_path = "s3://hrms-analytics-265475006349/processed/"

(
    loc_cust_df
    .write
    .format("delta")     # specify delta format
    .mode("overwrite")   # or "append"
    .option("mergeSchema", "true")
    .partitionBy("year", "month", "day")
    .save(output_path)
)

job.commit()

# STOP SPARK SESSION
spark.stop()