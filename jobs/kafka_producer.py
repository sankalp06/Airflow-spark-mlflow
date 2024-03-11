from confluent_kafka import Producer
import json
import time

def produce_to_kafka(bootstrap_servers, json_file, topic, batch_size):
    conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(**conf)

    try:
        with open(json_file, 'r') as file:
            json_objects = json.load(file)
    except FileNotFoundError:
        print(f"File '{json_file}' not found.")
        return

    batch = []
    for obj in json_objects:
        batch.append(obj)
        if len(batch) >= batch_size:
            for message in batch:
                producer.produce(topic, json.dumps(message).encode('utf-8'))
            print(f"Produced batch of {len(batch)} messages to Kafka topic")
            batch = []
            time.sleep(1)  # Adjust as needed

    # Send any remaining messages in the last batch
    if batch:
        for message in batch:
            producer.produce(topic, json.dumps(message).encode('utf-8'))
        print(f"Produced batch of {len(batch)} messages to Kafka topic")

    producer.flush()



if __name__ == "__main__":
    bootstrap_servers = 'host.docker.internal:9092'
    json_file = '/opt/airflow/data/producer.json'
    topic = 'healthcare_patients'
    batch_size = 10  # Adjust as needed

    produce_to_kafka(bootstrap_servers, json_file, topic, batch_size)
