# General Format of a Sink Connector with Partition Keys, (s3) type, format type (parquet) here, 
# Getting Dataframe to DynamicFrame (FromDF)

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext, SparkConf
from awsglue.job import Job
from datetime import datetime
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

dynamic_frame = DynamicFrame.fromDF(weather_df, glueContext, "dynamic_frame")
    sink = glueContext.getSink(
        connection_type="s3",
        path="____________",
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["____","_____","_____", "_____", "_____","____"],
        format_options={"useGlueParquetWriter": True}
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase="_______", catalogTableName="______")
    sink.writeFrame(dynamic_frame)
