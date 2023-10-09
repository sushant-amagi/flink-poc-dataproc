from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import RestartStrategies
import os
import json
import csv
from google.cloud import storage


env = StreamExecutionEnvironment.get_execution_environment()
env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 1000))

flink_kafka_connector_jar = '/home/jupyter/Sushant/flink-connector-kafka-1.17.1.jar'
kafka_client_jar = '/home/jupyter/Sushant/kafka-clients-2.8.0.jar'

# env.add_jars("file://" + flink_kafka_connector_jar, "file://" + kafka_client_jar)

# Initialize the GCS client outside the function to reuse the client for multiple writes
storage_client = storage.Client()

def write_to_gcs(value):
    bucket_name = "gs://test-kv/"
    blob_name = "/home/jupyter/Sushant/flink-output.csv"
    #client = storage.Client.from_service_account_json('path_to_key.json')
    
    # Initialize GCS client within the function
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    parsed_value = json.loads(value)
    header = parsed_value.keys()

    # Fetch the bucket
    #bucket = storage_client.bucket(bucket_name)

    # Check if the blob exists to decide on writing the header
    blob = bucket.blob(blob_name)
    if not blob.exists():
        with blob.open("w") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerow(parsed_value)
    else:
        # Append to the existing blob
        with blob.open("a") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writerow(parsed_value)
    
    return value



def write_to_file(value):
    parsed_value = json.loads(value)
    # CSV write
    csv_file = '/home/jupyter/Sushant/flink-output.csv'
    header = parsed_value.keys()
    # Check if file exists to write headers
    write_header = not os.path.exists(csv_file)
    with open(csv_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if write_header:
            writer.writeheader()
        writer.writerow(parsed_value)
    return value

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    properties = {
        "bootstrap.servers": "10.142.0.10:9092",
        "group.id": "test-group",
    }
    schema = SimpleStringSchema()

    kafka_consumer = FlinkKafkaConsumer("BeaconData26", schema, properties)
    kafka_consumer.set_start_from_earliest()

    stream = env.add_source(kafka_consumer)
    
    #stream.print()
    stream.map(write_to_gcs, output_type=Types.STRING())

    env.execute("Kafka to File")

if __name__ == '__main__':
    main()
