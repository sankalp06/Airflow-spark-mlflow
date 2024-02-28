from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
import json

def consume_from_kafka_and_insert_to_mongodb(bootstrap_servers, topic, group_id, mongo_uri, mongo_db_name, mongo_collection_name, message_limit, batch_size):
    # Create Kafka consumer instance
    bootstrap_servers = 'host.docker.internal:9092'
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    # MongoDB client and collection setup with authentication
    mongo_client = MongoClient(mongo_uri)
    db = mongo_client[mongo_db_name]
    collection = db[mongo_collection_name]

    # Number of messages consumed and batch initialization
    message_count = 0
    batch = []

    # Consume messages from Kafka topic and insert into MongoDB
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    # Error
                    print("Consumer error:", msg.error())
                    break
            else:
                # Message value (JSON object) decoding
                try:
                    json_data = json.loads(msg.value().decode('utf-8'))
                    batch.append(json_data)
                    message_count += 1

                    if len(batch) >= batch_size:
                        # Perform bulk insertion
                        collection.insert_many(batch)
                        print("Inserted data into MongoDB:", batch)
                        batch = []  # Reset batch

                    if message_count >= message_limit:
                        print(f"Reached message limit of {message_limit}. Stopping consumer.")
                        break

                except Exception as e:
                    print("Error processing message:", str(e))

    except KeyboardInterrupt:
        pass

    finally:
        # Insert remaining documents in batch
        if batch:
            collection.insert_many(batch)
            #print("Inserted data into MongoDB:", batch)

        # Clean up Kafka consumer and MongoDB client
        consumer.close()
        mongo_client.close()
