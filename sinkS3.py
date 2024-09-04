import boto3
from confluent_kafka import Consumer, KafkaError

# Configuration for Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 's3-sink-group',            
    'auto.offset.reset': 'earliest'         
}

# Kafka topic to consume from
topic_name = 'mytopic.my_db.test'


s3_bucket_name = 'databucketcdc'
s3_file_key = 'data/cdc.json' 


# AWS Credentials
aws_access_key_id = 'your aws key'
aws_secret_access_key = 'your aws key'
aws_region = 'us-east-1'
# Initialize S3 client
s3_client = boto3.client('s3')

# Initialize Kafka consumer
consumer = Consumer(kafka_config)
consumer.subscribe([topic_name])

def process_message(msg):
    # Assuming msg.value() is in JSON format
    data = msg.value().decode('utf-8')

    # Upload data to S3 (update or create the file)
    s3_client.put_object(Bucket=s3_bucket_name, Key=s3_file_key, Body=data)
    print(f"Uploaded data to S3 bucket '{s3_bucket_name}' with key '{s3_file_key}'")

def main():
    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for new messages

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Process the message and update S3
            process_message(msg)

    except KeyboardInterrupt:
        print("Process interrupted")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
