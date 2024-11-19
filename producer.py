from confluent_kafka import Producer

producer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
}


producer = Producer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


topic = 'test_topic'
for i in range(10):
    message = f"Test message {i}"
    producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
    producer.flush()

producer.flush()