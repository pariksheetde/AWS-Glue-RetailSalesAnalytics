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
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{SOURCE_BUCKET}{FOLDER}/{file_name}")

# 2. Rename columns
rename_map = {"load_ts": "load_timestamp"}
for old_name, new_name in rename_map.items():
    if old_name in df.columns:
        df = df.withColumnRenamed(old_name, new_name)

# 3. Add metadata
df = df.withColumn("ingestion_timestamp", current_timestamp()) \
       .withColumn("file_name", lit(file_name))

# 4. Register as temp view
df.createOrReplaceTempView("locations_temp_csv")

# 5. Query and display
spark.sql("SELECT * FROM locations_temp_csv").show(10, truncate=False)

# Commit Glue job
job.commit()

# Stop Spark session
spark.stop()