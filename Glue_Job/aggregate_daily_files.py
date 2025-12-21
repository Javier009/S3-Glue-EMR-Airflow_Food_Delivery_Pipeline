import sys
from awsglue.utils import getResolvedOptions
from datetime import datetime, timedelta
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Loading functions from utilities.py
from utilities import *


# Buckets to be used
RAW_DATA_BUCKET = "food-delvery-raw-data-bucket"
AGG_DATA_BUCKET = "food-delvery-raw-data-dailyagg-bucket"

# Identify partitions in raw data that are missing in aggregated data bucket
raw_partitions = list_partitions_ymd(RAW_DATA_BUCKET)
agg_partitions = list_partitions_ymd(AGG_DATA_BUCKET)

missing_partitions = []

for part in raw_partitions:
    year, month, day = part
    prefix = f"year={year}/month={month}/day={day}/"
    if not partition_has_output(AGG_DATA_BUCKET, prefix):
        missing_partitions.append(part)

# --- 1. Parameter Handling ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Set up contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


if not missing_partitions:
    print("No new folders to process. Exiting job.")
    job.commit()
    sys.exit(0)

for folder in missing_partitions:

    YEAR, MONTH, DAY = folder
    date_str = f"{YEAR}-{MONTH}-{DAY}"
    print(f"Processing date: {date_str}")

    # --- 2. Input and Output Paths ---
    INPUT_BUCKET = 'food-delvery-raw-data-bucket'
    INPUT_PATH = f"s3://{INPUT_BUCKET}/year={YEAR}/month={MONTH}/day={DAY}/" # No need to specify .json since all files in the folder are json files

    OUTPUT_BUCKET = 'food-delvery-raw-data-dailyagg-bucket'
    OUTPUT_PATH = f"s3://{OUTPUT_BUCKET}/year={YEAR}/month={MONTH}/day={DAY}"

    print(f"Reading from: {INPUT_PATH}")
    print(f"Writing to: {OUTPUT_PATH}")

    # --- 3. ETL and Compaction Logic ---
    # Read the many small JSON files for the specified date
    try:
        raw_df = (spark.read \
                .option("multiLine", "true") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .json(INPUT_PATH) \
                .cache()
                )

        if raw_df.rdd.isEmpty():
            print(f"WARNING: No raw data files found for date {date_str}. Committing job and exiting.")
            continue

        print(f"SUCCESS: Read {raw_df.count()} rows of data.")
        # Repartition to 1 file (This is the COMPACTION step)
        comp_df = raw_df.coalesce(1) # Number of output parquet files, set to 1 for full compaction

        # Write the aggregated data to S3 in Parquet format, partitioned by the date components
        comp_df.write.mode("overwrite").parquet(OUTPUT_PATH)

    except Exception as e:
        print(f"ERROR: Job failed due to: {str(e)}")
        sys.exit(1)

job.commit()