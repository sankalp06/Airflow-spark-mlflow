from confluent_kafka.admin import AdminClient, NewTopic

# Kafka broker configuration
conf = {'bootstrap.servers': 'host.docker.internal:9092'}

# Create AdminClient instance
admin_client = AdminClient(conf)

# Define topic name and replication factor
topic_name = 'healthcare_patients'
num_partitions = 1
replication_factor = 1  # Adjust as per your requirements

# Create NewTopic object
new_topic = NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
# Create topics asynchronously
create_topics_futures = admin_client.create_topics([new_topic])

# Wait for topic creation to finish
for topic_name, create_topic_future in create_topics_futures.items():
    try:
        create_topic_future.result()  # This will block until topic is created
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")
