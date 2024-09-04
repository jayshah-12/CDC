import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import max as spark_max

# Initialize Spark session with MySQL connector
spark = SparkSession.builder \
    .appName('Glue-MSSQL-Integration') \
    .config("spark.hadoop.fs.s3a.awsAccessKeyId", "AKIA5FTY7UITPCITKGN2") \
    .config("spark.hadoop.fs.s3a.awsSecretAccessKey", "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") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# MySQL database connection parameters
mysql_host = 'mysql'
mysql_port = 3306
mysql_user = 'root'
mysql_password = 'test'
mysql_db = 'my_db'
mysql_table = 'test'  # Source table
mysql_target_table = 'test2'  # Target table
checkpoint_file = "/tmp/checkpoint/last_processed_id.txt"  # Path to checkpoint file

# Define schema for MySQL table
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("marks", IntegerType(), True)
])

# Function to read the last processed ID
def get_last_processed_id():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            return int(f.read().strip())
    return 0

# Function to write the last processed ID
def set_last_processed_id(last_id):
    with open(checkpoint_file, 'w') as f:
        f.write(str(last_id))

# Function to read new data from MySQL
def read_from_mysql(last_id):
    query = f"(SELECT * FROM {mysql_table} WHERE id > {last_id}) AS temp"
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", query) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .load()
    return df

# Function to process and write data
def process_data():
    # Read the last processed ID
    last_id = get_last_processed_id()
    
    # Read new data from MySQL
    mysql_df = read_from_mysql(last_id)
    
    if mysql_df.count() > 0:
        # Show data in terminal (optional)
        mysql_df.show(truncate=False)
        
        # Write data to target MySQL table
        mysql_df.write.format("jdbc") \
            .option("url", f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", mysql_target_table) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .mode('append') \
            .save()
        
        # Update the last processed ID
        max_id = mysql_df.select(spark_max("id")).collect()[0][0]
        set_last_processed_id(max_id)

# Define a loop for continuous processing
while True:
    try:
        process_data()
    except Exception as e:
        print(f"Error occurred: {e}")
    
    # Sleep for a while before next poll
    time.sleep(60)  # Adjust the sleep time as needed

# Stop Spark session
spark.stop()
