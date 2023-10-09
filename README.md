# flink-poc-dataproc

1. create venv
2. Follow following steps

$export FLINK_HOME=/flink-1.17.1/

$FLINK_HOME/bin/start-cluster.sh

$FLINK_HOME/bin/flink run  --jarfile flink-connector-kafka-1.17.1.jar --jarfile kafka-clients-2.8.0.jar --jarfile flink-sql-connector-kafka-3.0.0-1.17.jar --jarfile flink-parquet-1.17.1.jar --jarfile flink-gs-fs-hadoop-1.17.1.jar -py flink_local.py 

