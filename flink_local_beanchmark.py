from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import RestartStrategies
import time
import psutil
import sys

env = StreamExecutionEnvironment.get_execution_environment()
env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 1000))

flink_kafka_connector_jar = '/home/jupyter/Sushant/flink-connector-kafka-1.17.1.jar'
kafka_client_jar = '/home/jupyter/Sushant/kafka-clients-2.8.0.jar'

global_start_time = time.time()
processed_records = 0
flink_completed = False  # Global flag to check if Flink has completed

def write_to_file(value):
    global processed_records
    with open('/home/jupyter/Sushant/flink-output.txt', 'a') as f:
        f.write(value + '\n')
    processed_records += 1
    return value

class PrintToFile:
    def __init__(self, file_name):
        self.file_name = file_name
        self.original_stdout = sys.stdout  # Store the original stdout

    def __enter__(self):
        sys.stdout = open(self.file_name, 'a')

    def __exit__(self, type, value, traceback):
        sys.stdout.close()
        sys.stdout = self.original_stdout  # Reset stdout to original

def main():
    global flink_completed
    with PrintToFile('/home/jupyter/Sushant/print-output.txt'):
        env = StreamExecutionEnvironment.get_execution_environment()
        properties = {
            "bootstrap.servers": "10.142.0.10:9092",
            "group.id": "test-group",
        }
        schema = SimpleStringSchema()

        kafka_consumer = FlinkKafkaConsumer("flink-read", schema, properties)
        kafka_consumer.set_start_from_earliest()

        stream = env.add_source(kafka_consumer)
        
        stream.print()
        stream.map(write_to_file, output_type=Types.STRING())

        env.execute("Kafka to File")
        
        flink_completed = True  # Set the flag to true after execution

        while not flink_completed:
            time.sleep(1)  # Wait for Flink to finish
        
        global_end_time = time.time()
        total_time = global_end_time - global_start_time

        # Measure Throughput
        throughput = processed_records / total_time
        print(f"Throughput: {throughput} records/sec")

        # Measure Latency
        latency = total_time / processed_records
        print(f"Average Latency: {latency} seconds/record")

        # Measure Resource Utilization
        cpu_percent = psutil.cpu_percent(interval=1)
        memory_info = psutil.virtual_memory()
        disk_io = psutil.disk_io_counters()

        print(f"CPU Utilization: {cpu_percent}%")
        print(f"Memory Utilization: {memory_info.percent}%")
        print(f"Disk Write Count: {disk_io.write_count}")
        print(f"Disk Write Bytes: {disk_io.write_bytes}")

if __name__ == '__main__':
    main()
