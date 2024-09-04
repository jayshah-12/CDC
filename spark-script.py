import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("S3 JSON Data Transformation") \
    .config("spark.hadoop.fs.s3a.access.key") \
    .config("spark.hadoop.fs.s3a.secret.key) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Define your S3 bucket and file path
input_s3_path = "s3://cdcbucket1234/topics/mytopic.my_db.test/partition=0/"
output_s3_path = "s3://output-databucket/output/"

# Read JSON data from S3
df = spark.read.json(input_s3_path)

# Perform some basic transformations
# 1. Calculate the average marks
average_marks = df.agg(avg("marks").alias("average_marks")).collect()[0]['average_marks']

# 2. Filter out records where marks are below the average
filtered_df = df.filter(col("marks") >= average_marks)

# 3. Add a new column with the status 'Passed' or 'Failed' based on the marks
result_df = filtered_df.withColumn("status", 
                                   when(col("marks") >= average_marks, "Passed")
                                   .otherwise("Failed"))

# Write the transformed data back to another S3 location in JSON format
result_df.write.mode("overwrite").json(output_s3_path)

# Stop the Spark session
spark.stop()