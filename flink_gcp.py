from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import RestartStrategies
import json
from google.cloud import storage
from pyflink.datastream import RichSinkFunction

class GcsSinkFunction(RichSinkFunction):
    
    def __init__(self, bucket_name, file_prefix):
        self.bucket_name = bucket_name
        self.file_prefix = file_prefix

    def invoke(self, value, context=None):
        client = storage.Client()
        bucket = client.get_bucket(self.bucket_name)
        import time
        data = str(value)
        filename = f"{self.file_prefix}_{int(time.time())}.txt"
        blob = bucket.blob(filename)
        blob.upload_from_string(data)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 1000))

    properties = {
        "bootstrap.servers": "10.142.0.10:9092",
        "group.id": "test-group",
    }

    schema = SimpleStringSchema()
    kafka_consumer = FlinkKafkaConsumer("BeaconData26", schema, properties)
    kafka_consumer.set_start_from_earliest()

    stream = env.add_source(kafka_consumer)
    
    # Define the sink's output path on GCS
    output_path = "asheesh-dev"
    
    # Use the custom GCS sink function
    gcs_sink = GcsSinkFunction(bucket_name=output_path, file_prefix='csv')
    
    # Process the incoming stream data, transform it as required
    def transform(value):
        parsed_value = json.loads(value)
        return json.dumps(parsed_value)  # Convert the parsed JSON back to a string format for the sink

    processed_stream = stream.map(transform, output_type=Types.STRING())
    processed_stream.add_sink(gcs_sink)

    env.execute("Kafka to GCS Bucket")

if __name__ == '__main__':
    main()

