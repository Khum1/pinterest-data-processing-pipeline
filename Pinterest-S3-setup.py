# Databricks notebook source
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

from pyspark.sql.functions import *
import urllib

# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0a4e65e909bd-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/pinterest_pipeline"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/pinterest_pipeline/topics/0a4e65e909bd.geo/partition=0/*.json"))

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

#set up of table joining geo and pin using the ind column
category_country_df = pin_df.join(geo_df, pin_df.ind == geo_df.ind, "inner")
category_country_df = category_country_df.groupBy("country", "category").count().withColumnRenamed("count", "category_count") 

sorted_df = category_country_df.orderBy(col("category_count").desc()) #sorting the df into being ordered by the highest category_count
most_popular_category_df = sorted_df.groupBy("country").agg( #sorting by each country
    first("category").alias("most_popular_category"), #gets the data frame from the first/most popular category
    first("category_count").alias("category_count")) #gets the category_count for the corresponding category

display(most_popular_category_df)

# COMMAND ----------

post_date_category_df = pin_df.join(geo_df, pin_df.ind == geo_df.ind, "inner")
post_date_category_df = post_date_category_df.groupBy("timestamp", "category").count().withColumnRenamed("count", "category_count")
post_year_category_df = post_date_category_df.withColumn("timestamp", post_date_category_df.timestamp.substr(1,4))
post_year_category_df = post_year_category_df.withColumnRenamed("timestamp", "post_year")
post_year_category_df = post_year_category_df.withColumn("post_year", col("post_year").cast("Integer"))
post_year_category_filtered_df = post_year_category_df.filter((col("post_year") >= 2018) & (col("post_year")<= 2022))

display(post_year_category_filtered_df)

# COMMAND ----------

country_poster_name_follower_df = pin_df.join(geo_df, pin_df.ind == geo_df.ind, "inner")
country_poster_name_follower_df = country_poster_name_follower_df.dropDuplicates(["country", "poster_name", "follower_count"])
country_poster_name_follower_df = country_poster_name_follower_df.select("country", "poster_name", "follower_count")
country_poster_name_follower_df = country_poster_name_follower_df.orderBy(col("follower_count").desc())
country_poster_name_follower_df = country_poster_name_follower_df.groupBy("country").agg(
    first("follower_count").alias("follower_count"),
    first("poster_name").alias("poster_name")
)

display(country_poster_name_follower_df)

# COMMAND ----------

most_followers_country_df = country_poster_name_follower_df.orderBy(col("follower_count").desc())
most_followers_country_df = most_followers_country_df.drop("poster_name")
most_followers_country_df = most_followers_country_df.head(1)

display(most_followers_country_df)

# COMMAND ----------

joined_pin_user_df = pin_df.join(user_df, pin_df.ind == user_df.ind, "inner")
category_age_df = joined_pin_user_df.select("category", "age")
category_age_df = category_age_df.withColumn("age_group", when((category_age_df.age >= 18) & (category_age_df.age <= 24), "18-24")
                                             .when((category_age_df.age >= 25) & (category_age_df.age <= 35), "25-35")
                                             .when((category_age_df.age >=36) & (category_age_df.age <= 50), "36-50")
                                             .when(category_age_df.age > 50, "50+")
                                             .otherwise("Unknown"))

grouped_category_age_df = category_age_df.groupBy("category", "age_group").count().withColumnRenamed("count", "category_count")
grouped_category_age_df = grouped_category_age_df.orderBy(col("category_count").desc())
most_popular_category_by_age_group_df = grouped_category_age_df.groupBy("age_group").agg(
    first("category").alias("category"),
    first("category_count").alias("category_count")
)

display(most_popular_category_by_age_group_df)

# COMMAND ----------

joined_pin_user_df = pin_df.join(user_df, pin_df.ind == user_df.ind, "inner")
follower_age_df = joined_pin_user_df.select("follower_count", "age")
follower_age_df = follower_age_df.withColumn("age_group", when((follower_age_df.age >= 18) & (follower_age_df.age <= 24), "18-24")
                                             .when((follower_age_df.age >= 25) & (follower_age_df.age <= 35), "25-35")
                                             .when((follower_age_df.age >= 36) & (follower_age_df.age <= 50), "36-50")
                                             .when(follower_age_df.age > 50, "+50")
                                             .otherwise("Unknown"))

age_group_median_df = follower_age_df.groupBy("age_group").agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))

display(age_group_median_df) 

# COMMAND ----------

users_joined_2015_2020 = user_df.withColumn("post_year", user_df.date_joined.substr(1,4))
users_joined_2015_2020 = users_joined_2015_2020.groupBy("post_year").count().withColumnRenamed("count","number_users_joined")
users_joined_2015_2020 = users_joined_2015_2020.orderBy(col("post_year").asc())

display(users_joined_2015_2020)

# COMMAND ----------

joined_pin_user_df = pin_df.join(user_df, pin_df.ind == user_df.ind, "inner")
post_date_follower_df = joined_pin_user_df.select("date_joined", "follower_count")
post_year_follower_df = post_date_follower_df.withColumn("date_joined", post_date_follower_df.date_joined.substr(1,4))

year_follower_median_df = post_year_follower_df.groupBy("date_joined").agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))
year_follower_median_df = year_follower_median_df.withColumnRenamed("date_joined", "post_year").orderBy(col("post_year").asc())

display(year_follower_median_df)

# COMMAND ----------

joined_pin_user_df = pin_df.join(user_df, pin_df.ind == user_df.ind, "inner")
follower_age_date_df = joined_pin_user_df.select("follower_count", "age", "date_joined")
follower_age_date_df = follower_age_date_df.withColumn("age_group", when((follower_age_date_df.age >= 18) & (follower_age_date_df.age <= 24), "18-24")
                                             .when((follower_age_date_df.age >= 25) & (follower_age_date_df.age <= 35), "25-35")
                                             .when((follower_age_date_df.age >= 36) & (follower_age_date_df.age <= 50), "36-50")
                                             .when(follower_age_date_df.age > 50, "+50")
                                             .otherwise("Unknown"))

follower_age_year_df = follower_age_date_df.withColumn("post_year", follower_age_date_df.date_joined.substr(1, 4)).drop("age")
follower_age_year_median_df = follower_age_year_df.groupBy("age_group", "post_year").agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))

display(follower_age_year_median_df)


# COMMAND ----------


