import sys
from awsglue.utils import getResolvedOptions
from datetime import datetime, timedelta
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# --- 1. Parameter Handling ---
args = getResolvedOptions(sys.argv, ['JOB_NAME']) # Optional 'date_to_process'

# Set up contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Determine the date to process
# If 'date_to_process' is provided, use it. Otherwise, calculate yesterday's date.
if 'date_to_process' in args and args['date_to_process']:
    date_str = args['date_to_process']
    print(f"Running AD-HOC job for date: {date_str}")
else:
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')
    print(f"Running SCHEDULED job for yesterday's date: {date_str}")

# Extract path components
YEAR = date_str[:4]
MONTH = date_str[5:7]
DAY = date_str[8:10]

# --- 2. Input and Output Paths ---
INPUT_BUCKET = 'food-delvery-raw-data-bucket'
INPUT_PATH = f"s3://{INPUT_BUCKET}/year={YEAR}/month={MONTH}/day={DAY}/*.json"

OUTPUT_BUCKET = 'food-delvery-raw-data-dailyagg-bucket'
OUTPUT_PATH = f"s3://{OUTPUT_BUCKET}/year={YEAR}/month={MONTH}/food_delivery_data_{date_str}.parquet"

print(f"Reading from: {INPUT_PATH}")
print(f"Writing to: {OUTPUT_PATH}")

# --- 3. ETL and Compaction Logic ---

# Read the many small JSON files for the specified date
# Assuming your raw data is JSON, adjust format if needed (e.g., 'csv', 'json')
try:
    raw_df = spark.read.json(INPUT_PATH)

    if raw_df.rdd.isEmpty():
        print(f"WARNING: No raw data files found for date {date_str}. Committing job and exiting.")
        job.commit()
        sys.exit(0)

    print(f"SUCCESS: Read {raw_df.count()} rows of data.")
    # Repartition to 1 file (This is the COMPACTION step)
    comp_df = raw_df.repartition(1) # Numebr of output parquet files, set to 1 for full compaction

    # Write the aggregated data to S3 in Parquet format, partitioned by the date components
    comp_df.write.mode("overwrite").parquet(OUTPUT_PATH)

    job.commit()
except Exception as e:
    print(f"ERROR: Job failed due to: {str(e)}")
    sys.exit(1)