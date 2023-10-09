from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import RestartStrategies
import pandas as pd
import pyarrow as pa
from pyflink.common import Row
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream.functions import MapFunction
import os
import json
import csv

class WriteToParquet(MapFunction):

    def __init__(self, base_output_path):
        self.base_output_path = base_output_path
        self.frames = []
        self.batch_size = 5000  # Adjust based on your needs
        self.file_counter = 0

    def map(self, value: Row) -> Row:
        parsed_value = json.loads(value[0])
        self.frames.append(parsed_value)
        if len(self.frames) >= self.batch_size:
            self.flush()
        return value

    def flush(self):
        df = pd.DataFrame(self.frames)
        table = pa.Table.from_pandas(df)
        
        # Construct dynamic path based on the counter
        current_output_path = f"{self.base_output_path}_{self.file_counter}.parquet"
        
        with pa.OSFile(current_output_path, 'wb') as sink:
            with pa.RecordBatchFileWriter(sink, table.schema) as writer:
                writer.write_table(table)
        
        # Increment counter and if it's 100, reset to 0
        self.file_counter = (self.file_counter + 1) % 100
        self.frames.clear()

    def close(self):
        if self.frames:
            self.flush()


            
env = StreamExecutionEnvironment.get_execution_environment()
env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 1000))

def main():
    properties = {
        "bootstrap.servers": "10.142.0.10:9092",
        "group.id": "test-group1",
    }
    schema = SimpleStringSchema()

    kafka_consumer = FlinkKafkaConsumer("BeaconData31", schema, properties)
    kafka_consumer.set_start_from_earliest()

    stream = env.add_source(kafka_consumer)
    
    # Define path for Parquet output
    output_path = '/home/jupyter/Sushant/parquet/flink-output.parquet'
    
    # Transform data and write to Parquet
    stream.map(WriteToParquet(output_path), output_type=RowTypeInfo([Types.STRING()]))
    
    env.execute("Kafka to Parquet")

if __name__ == '__main__':
    main()
