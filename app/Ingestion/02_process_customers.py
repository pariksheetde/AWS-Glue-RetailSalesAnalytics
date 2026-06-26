"""
This script is an AWS Glue job that processes customer data from a CSV file stored in an S3 bucket. It performs the following steps:
1. Reads the `customers.csv` file from the specified S3 bucket and folder.
2. Renames the `load_ts` column to `load_timestamp` for consistency.
3. Adds metadata columns: `ingestion_timestamp` (current timestamp) and `file_name` (name of the source file).
4. Writes the transformed data back to a specified S3 bucket in CSV format.
"""


import sys
import re
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_BUCKET = "s3://hrms-oracle-265475006349/bronze/"
FOLDER = "customers"
file_name = "customers.csv"

# 1. Load the data
customers_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"{SOURCE_BUCKET}{FOLDER}/{file_name}")

# 2. Add metadata
customers_trans_df = customers_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                 .withColumn("file_name", lit(file_name))

bucket_name = "hrms-oracle-265475006349"
final_prefix = "bronze/raw/customers/"

# 3. Write directly into final folder (Spark will create part files here)
customers_trans_df.write.options(header=True).format("csv").mode("overwrite").save(f"s3://{bucket_name}/{final_prefix}")

# 4. Rename part files in place with auto-increment pattern
s3 = boto3.client("s3")

# Find existing customers_NN.csv files to determine next suffix
objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=final_prefix)
max_suffix = 0
pattern = re.compile(r"customers_(\d+)\.csv")

for obj in objects.get("Contents", []):
    key = obj["Key"]
    match = pattern.search(key)
    if match:
        suffix = int(match.group(1))
        max_suffix = max(max_suffix, suffix)

next_suffix = max_suffix + 1

# Rename each part file to customers_XX_partN.csv
part_num = 1
for obj in objects.get("Contents", []):
    key = obj["Key"]
    if key.endswith(".csv") and "part" in key:  # Spark part files
        target_key = f"{final_prefix}customers_{next_suffix:02d}_part{part_num}.csv"
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": key},
            Key=target_key
        )
        s3.delete_object(Bucket=bucket_name, Key=key)  # remove original part file
        part_num += 1

print(f"Created {part_num-1} files with prefix customers_{next_suffix:02d}_partN.csv")

# Commit Glue job
job.commit()

# Stop Spark session
spark.stop()