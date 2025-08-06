import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_glue
accelerometer_glue_node1754427352844 = glueContext.create_dynamic_frame.from_catalog(database="accelerometer_landing", table_name="accelerometer_landing", transformation_ctx="accelerometer_glue_node1754427352844")

# Script generated for node share_with_research_as_of_date_query
SqlQuery0 = '''
SELECT a.*
FROM myDataSource AS a
LEFT JOIN customer_landing.customer_trusted AS c
  ON a.user = c.email
WHERE c.sharewithresearchasofdate IS NOT NULL
'''
share_with_research_as_of_date_query_node1754427359284 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":accelerometer_glue_node1754427352844}, transformation_ctx = "share_with_research_as_of_date_query_node1754427359284")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=share_with_research_as_of_date_query_node1754427359284, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754427344808", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1754427361748 = glueContext.getSink(path="s3://accelerometer-landing-wgu", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1754427361748")
accelerometer_trusted_node1754427361748.setCatalogInfo(catalogDatabase="accelerometer_landing",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1754427361748.setFormat("json")
accelerometer_trusted_node1754427361748.writeFrame(share_with_research_as_of_date_query_node1754427359284)
