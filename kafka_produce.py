from confluent_kafka import Producer
import json

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def main():
    # Define Kafka configuration
    conf = {
        'bootstrap.servers': '10.142.0.10:9092',  # Adjust with your server address
        'client.id': 'python-producer'
    }

    producer = Producer(conf)

    with open('/home/pradeepa_kv/kafka-platform/kafka_2.12-3.5.0/bin/file.json', 'r') as file:
        for line in file:
            try:
                record = json.loads(line)
                producer.produce('flink-read', key=None, value=json.dumps(record), callback=delivery_report)
            except json.JSONDecodeError:
                print("Invalid JSON: ", line)

            # Wait up to 1 second for events. Callbacks will be invoked during
            # this method call if the message is acknowledged.
            producer.poll(1)

    # Wait for any outstanding messages to be delivered and delivery reports to be received.
    producer.flush()

if __name__ == '__main__':
    main()
