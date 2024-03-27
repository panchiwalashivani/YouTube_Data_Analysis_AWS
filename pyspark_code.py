import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Data_Source
Data_Source_node1685352206564 = glueContext.create_dynamic_frame.from_catalog(database="youtube-data-analysis-raw", table_name="raw_statistics", transformation_ctx="Data_Source_node1685352206564", push_down_predicate="region in ('ca','gb','us')")

# Script generated for node Change Schema
ChangeSchema_node1685353487597 = ApplyMapping.apply(frame=Data_Source_node1685352206564, mappings=[("video_id", "string", "video_id", "string"), ("trending_date", "string", "trending_date", "string"), ("title", "string", "title", "string"), ("channel_title", "string", "channel_title", "string"), ("category_id", "bigint", "category_id", "bigint"), ("publish_time", "string", "publish_time", "string"), ("tags", "string", "tags", "string"), ("views", "bigint", "views", "bigint"), ("likes", "bigint", "likes", "bigint"), ("dislikes", "bigint", "dislikes", "bigint"), ("comment_count", "bigint", "comment_count", "bigint"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "boolean", "comments_disabled", "string"), ("ratings_disabled", "boolean", "ratings_disabled", "string"), ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"), ("description", "string", "description", "string"), ("region", "string", "region", "string")], transformation_ctx="ChangeSchema_node1685353487597")

# Drop null fields
DropNullFields_node = DropNullFields.apply(frame=ChangeSchema_node1685353487597, transformation_ctx="DropNullFields_node")

# Convert DynamicFrame to DataFrame
df = DropNullFields_node.toDF()

# Coalesce DataFrame
df_coalesced = df.coalesce(1)

# Convert DataFrame back to DynamicFrame
final_output_dynamic_frame = DynamicFrame.fromDF(df_coalesced, glueContext, "final_output_dynamic_frame")

# Script generated for node Catalog
Catalog_node1685352920969 = glueContext.getSink(path="s3://youtube-data-analysis-cleansed-useast1/youtube/raw_statistics/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region"], enableUpdateCatalog=True, transformation_ctx="Catalog_node1685352920969")
Catalog_node1685352920969.setCatalogInfo(catalogDatabase="youtube-data-analysis-raw",catalogTableName="youtube-data-analysis-cleansed-csv-to-parquet")
Catalog_node1685352920969.setFormat("glueparquet", compression="snappy")

# Write the final output DynamicFrame to the sink
Catalog_node1685352920969.writeFrame(final_output_dynamic_frame)

# Commit the job
job.commit()
