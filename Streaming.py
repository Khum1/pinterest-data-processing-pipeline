# Databricks notebook source
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

from pyspark.sql.functions import *
import urllib

file_type = "csv"
first_row_is_header = "true"
delimiter = ","
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']
# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True) #Deletes the checkpoint folder so that the write command can be run again

def create_dataframe_from_stream_data(type_of_record):
    '''
    Creates a dataframe from the incoming streaming data and produces a dataframe in a json string format

    Parameters
    ----------
    type_of_record: str
        the type of record that is being added to the df e.g. "pin"

    Returns
    -------
    df
        the dataframe that has been created
    '''
    df = spark.readStream \
        .format("kinesis") \
        .option("streamName", f"streaming-0a4e65e909bd-{type_of_record}") \
        .option("region", "us-east-1") \
        .option("initialPosition", 'earliest') \
        .option("awsAccessKey", ACCESS_KEY) \
        .option("awsSecretKey", SECRET_KEY) \
        .load()
    df = df.selectExpr("CAST(data as STRING)")
    return df

def normalise_follower_count():
    '''
    Changes the follower count from a string, to an integer

    Parameters
    ----------
    None

    Returns
    -------
    None
    '''
    df = pin_df.withColumn("follower_count", when(
        col("follower_count").rlike("\\d+k"), #Checks if the value matches a pattern of one or more digits plus the letter k (e.g. 12k)
        (regexp_extract(col("follower_count"), "(\\d+)", 1).cast("integer") * 1000) 
    ).otherwise(col("follower_count").cast("integer"))) 
    return df

def create_delta_table(df, type_of_record):
    df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
    .table(f"0a4e65e909bd_{type_of_record}_table")

schema = StructType([ #Gives the structure of the df for the table to be laid out 
    StructField("index",StringType(),True), 
    StructField("unique_id",StringType(),True), 
    StructField("title",StringType(),True), 
    StructField("description", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("follower_count", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("downloaded", StringType(), True),
    StructField("save_location", StringType(), True),
    StructField("category", StringType(), True)
  ])

pin_df = create_dataframe_from_stream_data("pin")

#Assembles data into df with separate columns
pin_df = pin_df.withColumn("jsonData",from_json(col("data"),schema)) \
                   .select("jsonData.*")

pin_df = normalise_follower_count()
pin_df = pin_df.withColumn("save_location", regexp_extract(col("save_location"), "(/data/).*", 0)) #Extracts the save location of the column 
pin_df = pin_df.withColumnRenamed("index", "ind") #Renames the index column to ind to match the geo and user dfs
# pin_df = pin_df.withColumn("ind",col("ind").cast("integer")) #Ensures ind column is an integer

column_structure = ["ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category"]
pin_df = pin_df.select(column_structure) #Rstructures the column to the order in column_structure

display(pin_df)
create_delta_table(pin_df, "pin")


# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType

schema = StructType([ 
    StructField("ind",StringType(),True), 
    StructField("latitude",StringType(),True), 
    StructField("longitude",StringType(),True), 
    StructField("country", StringType(), True),
    StructField("timestamp", StringType(), True)
  ])

geo_df = create_dataframe_from_stream_data("geo")
geo_df = geo_df.withColumn("jsonData",from_json(col("data"),schema)) \
                   .select("jsonData.*")

geo_df = geo_df.withColumn("coordinates", array(col("longitude"), col("latitude")))
geo_df = geo_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
column_structure = ["ind", "country", "coordinates", "timestamp"]
geo_df = geo_df.select(column_structure)

display(geo_df)
create_delta_table(geo_df, "geo")

# COMMAND ----------

schema = StructType([ 
    StructField("ind",StringType(),True), 
    StructField("first_name",StringType(),True), 
    StructField("last_name",StringType(),True), 
    StructField("age", StringType(), True),
    StructField("date_joined", StringType(), True)
  ])

user_df = create_dataframe_from_stream_data("user")
user_df = user_df.withColumn("jsonData",from_json(col("data"),schema)) \
                   .select("jsonData.*")

user_df = user_df.withColumn("user_name", concat(col("first_name"),col("last_name")))
user_df = user_df.drop("first_name", "last_name")
user_df = user_df.withColumn("date_joined", col("date_joined").cast("timestamp"))

column_structure = ["ind", "user_name", "age", "date_joined"]
user_df = user_df.select(column_structure)

display(user_df)
create_delta_table(user_df, "user")

# COMMAND ----------


