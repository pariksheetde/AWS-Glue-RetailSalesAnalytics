import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit

# Initialize Spark and Glue Context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_BUCKET = "hrms"
FOLDER = "locations"
file_name = "locations.csv"

# 1. Load the data FIRST
# Use .option("inferSchema", "true") if you want Spark to guess data types (int, double, etc.)
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"s3://{SOURCE_BUCKET}/{FOLDER}/{file_name}")

# 2. Define the rename mapping
rename_map = {
    "circuitId": "circuit_id",
    "circuitRef": "circuit_ref",
    "lat": "latitude",
    "lng": "longitude",
    "alt": "altitude",
}

# 3. Apply renames and add metadata
for old_name, new_name in rename_map.items():
    if old_name in df.columns:
        df = df.withColumnRenamed(old_name, new_name)

# Adding ingestion timestamp (Standard practice in Glue ETL)
df = df.withColumn("ingestion_timestamp", current_timestamp())\
       .withColumn("file_name", lit(file_name))

# 4. Register as a temporary view
df.createOrReplaceTempView("locations_temp_csv")

# 5. Query and Display
spark.sql("""SELECT 
             circuit_id,
             circuit_ref,
             name,
             location,
             country,
             latitude,
             longitude,
             altitude,
             ingestion_timestamp,
             file_name
             FROM locations_temp_csv""").show(10, truncate=False)