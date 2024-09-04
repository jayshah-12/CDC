from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def main():
    # Initialize Spark session with JDBC driver
    spark = SparkSession.builder \
        .appName('Glue-MSSQL-Integration') \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", "AKIA5FTY7UITPCITKGN2") \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", "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") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    my_url = "jdbc:mysql://mysql:3306/my_db"
    

    mysql_options = {
        "user": "root",
        "password": "test",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

   
    df = spark.read.jdbc(url=my_url, table="test", properties=mysql_options)
    
   
    df = df.drop("date")
    df_filtered = df.filter(col('marks') > 50)
    
    
    df_transformed = df_filtered.withColumn(
        'Grade',
        when(col('marks') >= 75, 'A+')
        .when((col('marks') >= 60) & (col('marks') < 75), 'A')
        .otherwise('B')
    )
    
    
    mssql_url = "jdbc:sqlserver://mssql:1433;databaseName=tempdb"
    mssql_options = {
        "user": "sa",
        "password": "Jayshah12@",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    
    df_transformed.write \
        .mode('overwrite') \
        .jdbc(url=mssql_url, table="temptable", properties=mssql_options)
 
    spark.stop()

if __name__ == "__main__":
    main()