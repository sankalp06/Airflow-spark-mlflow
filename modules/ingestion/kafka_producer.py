from confluent_kafka import Producer
import json

def produce_to_kafka(bootstrap_servers,json_file, topic):
    bootstrap_servers = 'host.docker.internal:9092'
    # Create Kafka Producer instance
    conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(conf)

    # Function to handle delivery reports from Kafka broker
    def delivery_report(err, msg):
        if err is not None:
            print('Message delivery failed:', err)
        else:
            print('Message delivered to topic:', msg.topic())

    # Function to read JSON objects from file and produce them to Kafka topic
    def produce_message():
        try:
            with open(json_file, 'r') as file:
                json_objects = json.load(file)
        except FileNotFoundError:
            print(f"File '{json_file}' not found.")
            return

        # Produce JSON objects to Kafka topic
        for obj in json_objects:
            producer.produce(topic, key=None, value=json.dumps(obj), callback=delivery_report)

        # Wait for all messages to be delivered
        producer.flush()

    # Run producer
    produce_message()



