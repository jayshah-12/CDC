import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName('Glue-S3-Transformation') \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", "YOUR_AWS_ACCESS_KEY_ID") \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", "YOUR_AWS_SECRET_ACCESS_KEY") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    
    input_path = 's3://cdcbucket123/topics/mytopic.my_db.test/partition=0/'
    output_path = 's3://cdcbucket123/output/'

    
    df = spark.read.json(input_path)

    
    df_filtered = df.filter(col('marks') > 50)

    df_transformed = df_filtered.withColumn(
        'status',
        when(col('marks') >= 75, 'Pass with Distinction')
        .when((col('marks') >= 60) & (col('marks') < 75), 'Pass')
        .otherwise('Fail')
    )

    
    df_transformed.write.mode('overwrite').json(output_path)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()