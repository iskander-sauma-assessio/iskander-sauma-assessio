import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import input_file_name
from awsglue.job import Job
from pyspark.sql.functions import explode, col, lit, to_timestamp
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','source','destination','env'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# function to upload data to s3/athena
def upload_to_athena(df, table_name, data_source):
    dyf = DynamicFrame.fromDF(df, glueContext, table_name)

    s3output = glueContext.getSink(
      path=f"s3://{data_source}/{table_name}",
      connection_type="s3",
      updateBehavior="UPDATE_IN_DATABASE",
      partitionKeys=[],
      compression="snappy",
      enableUpdateCatalog=True,
      transformation_ctx="s3output",
    )
    s3output.setCatalogInfo(
      catalogDatabase="assessment_database", catalogTableName=f"{table_name}"
    )
    s3output.setFormat("glueparquet")
    s3output.writeFrame(dyf)

# load bulk data from s3 bucket and print schema
dyf = glueContext.create_dynamic_frame_from_options(connection_type = "s3", 
                                                    connection_options = {"paths": [f"s3://{args['source']}/"]}, 
                                                    format="json")

# convert Dynamic frame to spark Dataframe, then add a column with random string as an id column
df = dyf.toDF()

# check if pyspark dataframe is empty
if df.count() == 0:
    sys.exit(0)

# create lenscores dataframe
df_lensscores = df.select(lit(col("id")), lit(col("payload.calculationVersion")).alias('calculationVersion'), 
                            explode('payload.lensScores').alias("lensScores"))
df_lensscores = df_lensscores.withColumn("lensId", col("lensScores.lensId"))
df_lensscores = df_lensscores.withColumn("lensName", col("lensScores.lensName"))
df_lensscores = df_lensscores.withColumn("matchScore", col("lensScores.matchScore"))
df_lensscores = df_lensscores.drop(col("lensScores"))
df_lensscores.show()

# create mapping dataframe
df_mapping = df.select('payload.lensScores')
# select only the first row as the lens names and lens id are the same over the rows
df_mapping = df_mapping.limit(1)
# explode the list into separate rows
df_mapping = df_mapping.select(explode("lensScores").alias("lensScore"))
# create the columns lensId and lensName
df_mapping = df_mapping.withColumn("lensId", col("lensScore.lensId"))
df_mapping = df_mapping.withColumn("lensName", col("lensScore.lensName"))
# drop lensScore as it is no longer needed
df_mapping = df_mapping.drop(col("lensScore"))

# create assessmentScores dataframe
df_assessment = df.select('id', 'payload.calculationVersion', 'payload.assessmentScores', 'payload.assessmentSubScores')
for i in range(len(df_assessment.select("assessmentScores").first()[0])):
    df_assessment = df_assessment.withColumn(f"temp_column_{i}", df_assessment["assessmentScores"].getItem(i))
    competency_name = df_assessment.select(col(f"temp_column_{i}.name")).first()[0]
    df_assessment = df_assessment.withColumn(competency_name, col(f"temp_column_{i}.value").cast('integer'))
    df_assessment = df_assessment.drop(col(f"temp_column_{i}"))
    
for i in range(len(df_assessment.select("assessmentSubScores").first()[0])):
    df_assessment = df_assessment.withColumn(f"temp_column_{i}", df_assessment["assessmentSubScores"].getItem(i))
    competency_name = df_assessment.select(col(f"temp_column_{i}.name")).first()[0]
    df_assessment = df_assessment.withColumn(competency_name, col(f"temp_column_{i}.value").cast('integer'))
    df_assessment = df_assessment.drop(col(f"temp_column_{i}"))
    
df_assessment = df_assessment.drop(col("assessmentScores"))
df_assessment = df_assessment.drop(col("assessmentSubScores"))

# create subcompetency dataframe
df_subcompetency = df.select('id', 'payload.calculationVersion', 'payload.competencySubScores')
# iterate over all indices in competencyScores
for i in range(len(df_subcompetency.select("competencySubScores").first()[0])):
    # create a temp column for each index and get the competency name
    df_subcompetency = df_subcompetency.withColumn(f"temp_column_{i}", df_subcompetency["competencySubScores"].getItem(i))
    competency_name = df_subcompetency.select(col(f"temp_column_{i}.key")).first()[0]
    # create a column of the competency name with the corresponding value and one for performance level
    df_subcompetency = df_subcompetency.withColumn(competency_name, col(f"temp_column_{i}.value"))  
    # delete temp created column
    df_subcompetency = df_subcompetency.drop(col(f"temp_column_{i}"))

# drop competencyScore as it is no longer needed
df_subcompetency = df_subcompetency.drop(col("competencySubScores"))
# some values can be missing, fill with zeroes
df_subcompetency = df_subcompetency.na.fill(value=0)

# create a dataframefor competency scores
df_competency = df.select('id', 'payload.calculationVersion', 'payload.competencyScores')
# iterate over all indices in competencyScores
for i in range(len(df_competency.select("competencyScores").first()[0])):
    # create a temp column for each index and get the competency name
    df_competency = df_competency.withColumn(f"temp_column_{i}", df_competency["competencyScores"].getItem(i))
    competency_name = df_competency.select(col(f"temp_column_{i}.key")).first()[0]
    # create a column of the competency name with the corresponding value and one for performance level
    df_competency = df_competency.withColumn(competency_name, col(f"temp_column_{i}.value"))  
    # delete temp created column
    df_competency = df_competency.drop(col(f"temp_column_{i}"))

# drop competencyScore as it is no longer needed
df_competency = df_competency.drop(col("competencyScores"))
# some values can be missing, fill with zeroes
df_competency = df_competency.na.fill(value=0)

# create demographics dataframe
df_demo = df.select('id', 'payload.mapLocale', 'payload.matrigmaLocale',
                    col('payload.mapCompletionDate').cast('timestamp'), 
                    col('payload.matrigmaCompletionDate').cast('timestamp'))

# create dataframe for when data was sent to firehose
df_creation = df.select('id', 'createdAt')

# create performance dataframe
df_performance = df.select(lit(col('id')), lit(col("payload.calculationVersion").alias("calculationVersion")),
                    explode('payload.competencyScores').alias("competencyScores"))
df_performance = df_performance.withColumn("CompetencyName", col("competencyScores.key"))
df_performance = df_performance.withColumn("CompetencyScore", col("competencyScores.value"))
df_performance = df_performance.withColumn("performanceLevel", col("competencyScores.performanceLevel"))
df_performance = df_performance.drop(col("competencyScores"))

#create performance dataframe for subscores
df_subperformance = df.select(lit(col('id')), lit(col("payload.calculationVersion").alias("calculationVersion")),
                        explode('payload.competencySubScores').alias("competencySubScores"))
df_subperformance = df_subperformance.withColumn("CompetencyName", col("competencySubScores.key"))
df_subperformance = df_subperformance.withColumn("CompetencyScore", col("competencySubScores.value"))
df_subperformance = df_subperformance.withColumn("performanceLevel", col("competencySubScores.performanceLevel"))
df_subperformance = df_subperformance.drop(col("competencySubScores"))

# merge performanceLevel dataframes
df_performance_all = df_performance.union(df_subperformance)

# Write the data in the dataframe to a location in Amazon S3 and a table for it in the AWS Glue Data Catalog
upload_to_athena(df_lensscores, 'lensScores', args['destination'])
upload_to_athena(df_mapping, 'lensNames', args['destination'])
upload_to_athena(df_assessment, 'assessmentScores', args['destination'])
upload_to_athena(df_competency, 'competencyScores', args['destination'])
upload_to_athena(df_subcompetency, 'competencySubScores', args['destination'])
upload_to_athena(df_demo, 'demographics', args['destination'])
upload_to_athena(df_creation, 'systemInfo', args['destination'])
upload_to_athena(df_performance_all, 'performanceLevel', args['destination'])

job.commit()