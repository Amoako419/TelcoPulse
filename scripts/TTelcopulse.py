import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame
import gs_null_rows
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1747134411981 = glueContext.create_data_frame.from_options(connection_type="kinesis",connection_options={"typeOfData": "kinesis", "streamARN": "arn:aws:kinesis:eu-west-1:405894843300:stream/dev-telcopulse-stream", "classification": "json", "startingPosition": "earliest", "inferSchema": "true"}, transformation_ctx="dataframe_AmazonKinesis_node1747134411981")

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        AmazonKinesis_node1747134411981 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        # Script generated for node Remove Null Rows
        RemoveNullRows_node1747134428380 = AmazonKinesis_node1747134411981.gs_null_rows(extended=True)

        # Script generated for node Aggregate
        Aggregate_node1747134446913 = sparkAggregate(glueContext, parentFrame = RemoveNullRows_node1747134428380, groups = ["operator", "postal_code"], aggs = [["signal", "avg"], ["precission", "avg"], ["status", "count"]], transformation_ctx = "Aggregate_node1747134446913")

        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour

        # Script generated for node Amazon S3
        AmazonS3_node1747134451846_path = "s3://dev-telcopulse-data/processed-data" + "/ingest_year=" + "{:0>4}".format(str(year)) + "/ingest_month=" + "{:0>2}".format(str(month)) + "/ingest_day=" + "{:0>2}".format(str(day)) + "/ingest_hour=" + "{:0>2}".format(str(hour))  + "/"
        AmazonS3_node1747134451846 = glueContext.write_dynamic_frame.from_options(frame=Aggregate_node1747134446913, connection_type="s3", format="glueparquet", connection_options={"path": AmazonS3_node1747134451846_path, "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1747134451846")

glueContext.forEachBatch(frame = dataframe_AmazonKinesis_node1747134411981, batch_function = processBatch, options = {"windowSize": "100 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"})
job.commit()