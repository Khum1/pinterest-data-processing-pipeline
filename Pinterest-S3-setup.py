# Databricks notebook source
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

from pyspark.sql.functions import *
# import urllib

# # Specify file type to be csv
# file_type = "csv"
# # Indicates file has first row as the header
# first_row_is_header = "true"
# # Indicates file has comma as the delimeter
# delimiter = ","
# # Read the CSV file to spark dataframe
# aws_keys_df = spark.read.format(file_type)\
# .option("header", first_row_is_header)\
# .option("sep", delimiter)\
# .load("/FileStore/tables/authentication_credentials.csv")

# # Get the AWS access key and secret key from the spark dataframe
# ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
# SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']
# # Encode the secrete key
# ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# # AWS S3 bucket name
# AWS_S3_BUCKET = "user-0a4e65e909bd-bucket"
# # Mount name for the bucket
# MOUNT_NAME = "/mnt/pinterest_pipeline"
# # Source url
# SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# # Mount the drive
# dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/pinterest_pipeline/topics/0a4e65e909bd.geo/partition=0/*.json"
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
geo_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)

display(geo_df)

geo_df = geo_df.withColumn("coordinates", array(col("longitude"), col("latitude")))

geo_df = geo_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

column_structure = ["ind", "country", "coordinates", "timestamp"]
geo_df = geo_df.select(column_structure)

# Display Spark dataframe to check its content

display(geo_df)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/pinterest_pipeline/topics/0a4e65e909bd.pin/partition=0/*.json"
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
pin_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)

pin_df = pin_df.replace("No description available Story format", None)
pin_df = pin_df.replace("null", None)
pin_df = pin_df.replace("User Info Error", None)
pin_df = pin_df.replace("Image src error.", None)
pin_df = pin_df.replace("No Title Data Available", None)
pin_df = pin_df.replace("N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", None)

pin_df = pin_df.withColumn("follower_count", when(
    col("follower_count").rlike("\\d+k"), #checks if the value matches a pattern of one or more digits plus the letter k
    (regexp_extract(col("follower_count"), "(\\d+)", 1).cast("integer") * 1000) #extracts the integer from the cells containing k and * 1000
).otherwise(col("follower_count").cast("integer"))) #if it doesn't contain a k, it leaves the integer value

pin_df = pin_df.withColumn("save_location", regexp_extract(col("save_location"), "(/data/).*", 0)) #extracts the save location of the column 

pin_df = pin_df.withColumnRenamed("index", "ind")

column_structure = ["ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category"]
pin_df = pin_df.select(column_structure)

print(pin_df.dtypes)
# Display Spark dataframe to check its content
display(pin_df)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/pinterest_pipeline/topics/0a4e65e909bd.user/partition=0/*.json"
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
user_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)

user_df = user_df.withColumn("user_name", concat(col("first_name"),col("last_name")))
user_df = user_df.drop("first_name", "last_name")
user_df = user_df.withColumn("date_joined", col("date_joined").cast("timestamp"))

column_structure = ["ind", "user_name", "age", "date_joined"]
user_df = user_df.select(column_structure)
# Display Spark dataframe to check its content
display(user_df)


# COMMAND ----------

def join_df(df1, df2):
    joined_df = df1.join(df2, df1.ind == df2.ind, "inner")
    return joined_df

def create_post_year(df, column):
    '''


    Parameters
    ----------
    df

    column : str
        the column that is to be changed to post_year
    '''
    df = df.withColumn(f"{column}", df[column].substr(1,4))
    df = df.withColumnRenamed(f"{column}", "post_year")
    df = df.withColumn("post_year", col("post_year").cast("Integer"))
    return df

def create_age_groups(df):
    df = df.withColumn("age_group", when((df.age >= 18) & (df.age <= 24), "18-24")
                                             .when((df.age >= 25) & (df.age <= 35), "25-35")
                                             .when((df.age >=36) & (df.age <= 50), "36-50")
                                             .when(df.age > 50, "50+")
                                             .otherwise("Unknown"))
    return df

def find_most_popular_category(df, column):
    df = df.groupBy(column, "category").count().withColumnRenamed("count", "category_count")
    sorted_df = df.orderBy(col("category_count").desc())
    df = sorted_df.groupBy(column).agg( #sorting by each country
    first("category").alias("most_popular_category"), #gets the data frame from the first/most popular category
    first("category_count").alias("category_count"))
    return df

# COMMAND ----------

#Most popular category in each country
category_country_df = join_df(pin_df, geo_df)
most_popular_category_df = find_most_popular_category(category_country_df, "country")

display(most_popular_category_df)

# COMMAND ----------

#Most popular category each year 
post_year_geo_df = create_post_year(geo_df, "timestamp")
post_year_category_df = join_df(post_year_geo_df, pin_df)
post_year_category_df = find_most_popular_category(post_year_category_df, "post_year")
post_year_category_filtered_df = post_year_category_df.filter((col("post_year") >= 2018) & (col("post_year")<= 2022))
most_popualr_category_by_year_df = post_year_category_filtered_df.orderBy(col("post_year").desc())

display(most_popualr_category_by_year_df)

# COMMAND ----------

# User with most followers in each country
country_poster_name_follower_df = join_df(pin_df, geo_df)
country_poster_name_follower_df = country_poster_name_follower_df.dropDuplicates(["country", "poster_name", "follower_count"])
country_poster_name_follower_df = country_poster_name_follower_df.select("country", "poster_name", "follower_count")

country_poster_name_follower_df = country_poster_name_follower_df.orderBy(col("follower_count").desc())
most_followers_by_country_df = country_poster_name_follower_df.groupBy("country").agg(
    first("follower_count").alias("follower_count"),
    first("poster_name").alias("poster_name")
)

display(most_followers_by_country_df)

# COMMAND ----------

# User with most followers overall
most_followers_country_df = country_poster_name_follower_df.orderBy(col("follower_count").desc())
most_followers_country_df = most_followers_country_df.drop("poster_name")
most_followers_overall_df = most_followers_country_df.head(1)

display(most_followers_overall_df)

# COMMAND ----------

# Most popular category for each age group
joined_pin_user_df = join_df(pin_df, user_df)
category_age_df = joined_pin_user_df.select("category", "age")
category_age_df = create_age_groups(category_age_df)
grouped_category_age_df = category_age_df.groupBy("category", "age_group").count().withColumnRenamed("count", "category_count")
grouped_category_age_df = grouped_category_age_df.orderBy(col("category_count").desc())
most_popular_category_by_age_group_df = grouped_category_age_df.groupBy("age_group").agg(
    first("category").alias("category"),
    first("category_count").alias("category_count")
)

display(most_popular_category_by_age_group_df)

# COMMAND ----------

# Median follower count for each age group
joined_pin_user_df = pin_df.join(user_df, pin_df.ind == user_df.ind, "inner")
follower_age_df = joined_pin_user_df.select("follower_count", "age")
follower_age_df = create_age_groups(follower_age_df)

age_group_median_df = follower_age_df.groupBy("age_group").agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))
age_group_median_df = age_group_median_df.orderBy(col("age_group").asc())

display(age_group_median_df) 

# COMMAND ----------

#Number of users joined each year
users_joined_2015_2020 = create_post_year(user_df, "date_joined")
users_joined_2015_2020 = users_joined_2015_2020.groupBy("post_year").count().withColumnRenamed("count","number_users_joined")
users_joined_2015_2020 = users_joined_2015_2020.orderBy(col("post_year").asc())

display(users_joined_2015_2020)

# COMMAND ----------

# Median follower count for users based on their join year
pin_user_df = join_df(pin_df, user_df)
post_date_follower_df = pin_user_df.select("date_joined", "follower_count")
post_year_follower_df = create_post_year(post_date_follower_df, "date_joined")

year_follower_median_df = post_year_follower_df.groupBy("post_year").agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))

display(year_follower_median_df)

# COMMAND ----------

#Median follower count based on age group and join year
pin_user_df = join_df(pin_df, user_df)
follower_age_date_df = joined_pin_user_df.select("follower_count", "age", "date_joined")
follower_age_date_df = create_age_groups(follower_age_date_df)
follower_age_year_df = follower_age_date_df.drop("age")
follower_age_year_df = create_post_year(follower_age_date_df, "date_joined")

follower_age_year_median_df = follower_age_year_df.groupBy("age_group", "post_year").agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))

display(follower_age_year_median_df)


# COMMAND ----------


