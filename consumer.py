from confluent_kafka import Consumer, KafkaError

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test_group',
    'auto.offset.reset': 'earliest',
    'enable.partition.eof': True
}


consumer = Consumer(consumer_conf)

topics = ['test_topic']
consumer.subscribe(topics)

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition {msg.partition()}")
            else:
                print(f"Consumer error: {msg.error()}")
            continue

        print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    consumer.close()