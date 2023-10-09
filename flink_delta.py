from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import RestartStrategies
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table import expressions as expr


env = StreamExecutionEnvironment.get_execution_environment()
env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 1000))

flink_kafka_connector_jar = '/home/jupyter/Sushant/flink-connector-kafka-1.17.1.jar'
kafka_client_jar = '/home/jupyter/Sushant/kafka-clients-2.8.0.jar'

# env.add_jars("file://" + flink_kafka_connector_jar, "file://" + kafka_client_jar)
# Create table environment
t_env = StreamTableEnvironment.create(env)


def main():
    global env 
    properties = {
        "bootstrap.servers": "10.142.0.10:9092",
        "group.id": "test-group",
    }
    schema = SimpleStringSchema()

    kafka_consumer = FlinkKafkaConsumer("flink-read", schema, properties)
    kafka_consumer.set_start_from_earliest()

    stream = env.add_source(kafka_consumer)
    
    # Convert the DataStream to a Table

    table = t_env.from_data_stream(stream).alias("value").select(expr.col("value").cast(DataTypes.STRING()))


    # Define the delta sink. This is hypothetical and might differ based on the actual connector's API.
    sink_ddl = """
    CREATE TABLE delta_sink (
        `value` STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'asheesh-dev/pyflink',
        'format' = 'parquet'
    )
    """

    t_env.execute_sql(sink_ddl)

    # Write table to delta format
    table.execute_insert("delta_sink")

    env.execute("Kafka to Delta File")

if __name__ == '__main__':
    main()
