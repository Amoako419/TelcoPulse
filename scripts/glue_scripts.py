import sys
import json
import boto3
import base64
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, TimestampType

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'kinesis_stream_name',
    'region_name',
    'window_size',
    'output_s3_path',
    'output_format'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set job parameters
kinesis_stream_name = args['kinesis_stream_name']
region_name = args['region_name']
window_size = int(args['window_size'])  # in seconds
output_s3_path = args['output_s3_path']
output_format = args['output_format']  # parquet, csv, json, etc.

# Schema for validation (adjust based on your data structure)
schema = StructType([
    StructField("id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("value", IntegerType(), True),
    StructField("metadata", StringType(), True)
])

# Function to decode and validate Kinesis record
def process_kinesis_record(record):
    # Decode base64 data from Kinesis
    try:
        payload = base64.b64decode(record['data']).decode('utf-8')
        data = json.loads(payload)
        
        # Basic validation
        required_fields = ['id', 'timestamp', 'event_type']
        for field in required_fields:
            if field not in data:
                return None  # Skip invalid records
        
        # Convert timestamp string to timestamp
        try:
            data['timestamp'] = datetime.strptime(data['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            try:
                data['timestamp'] = datetime.strptime(data['timestamp'], '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                return None  # Invalid timestamp format
        
        return data
    except Exception as e:
        print(f"Error processing record: {str(e)}")
        return None

# Create Kinesis data source
kinesis_source = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "streamName": kinesis_stream_name,
        "classification": "json",
        "startingPosition": "LATEST",
        "inferSchema": "false",
        "windowSize": window_size,
        "region": region_name
    }
)

# Process streaming data
def process_batch(data_frame, batch_id):
    if data_frame.count() > 0:
        # Convert to Spark DataFrame
        df = data_frame.toDF()
        
        # Apply UDF to decode and validate Kinesis records
        process_udf = udf(process_kinesis_record, schema)
        processed_df = df.withColumn("processed", process_udf(col("_raw")))
        
        # Extract fields from the processed column
        valid_records = processed_df.filter(col("processed").isNotNull()) \
            .select(
                col("processed.id"),
                col("processed.timestamp"),
                col("processed.customer_id"),
                col("processed.event_type"),
                col("processed.value"),
                col("processed.metadata")
            )
        
        # Apply transformations
        transformed_df = valid_records \
            .withColumn("processing_date", lit(datetime.now().strftime("%Y-%m-%d"))) \
            .withColumn("event_category", when(col("event_type").isin(["click", "view", "scroll"]), "engagement")
                                      .when(col("event_type").isin(["purchase", "add_to_cart"]), "conversion")
                                      .otherwise("other")) \
            .withColumn("value_normalized", when(col("value").isNotNull(), col("value")).otherwise(0))
        
        # Convert to DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_dynamic_frame")
        
        # Partition data by processing_date
        partition_key = "processing_date"
        
        # Write to S3
        s3_sink = glueContext.getSink(
            path=output_s3_path,
            connection_type="s3",
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=[partition_key],
            compression=output_format.upper() if output_format.lower() == "gzip" else "NONE",
            enableUpdateCatalog=True
        )
        s3_sink.setCatalogInfo(
            catalogDatabase="kinesis_processed_data",
            catalogTableName=f"{kinesis_stream_name}_processed"
        )
        s3_sink.setFormat(output_format)
        s3_sink.writeFrame(dynamic_frame)
        
        print(f"Successfully processed batch {batch_id} with {transformed_df.count()} records")

# Start the streaming job
glueContext.forEachBatch(
    frame=kinesis_source,
    batch_function=process_batch,
    options={
        "windowSize": window_size,
        "checkpointLocation": f"{output_s3_path}_checkpoint/"
    }
)

job.commit()