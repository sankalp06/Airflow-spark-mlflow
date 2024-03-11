# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, current_timestamp
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
# from confluent_kafka import Consumer, KafkaError
# from pymongo import MongoClient
# import json

# def consume_from_kafka_and_insert_to_mongodb():
#     spark = SparkSession.builder \
#         .appName("KafkaMongoSparkStreaming") \
#         .master("spark://spark-master:7077") \
#         .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:10.2.2")\
#         .config("spark.mongodb.input.uri=mongodb://root:root@host.docker.internal:27017/healthcare_db.spark_test") \
#         .config("spark.mongodb.write.connection.uri", "mongodb://root:root@host.docker.internal:27017/")\
#         .getOrCreate()

#     # Define the schema based on the provided structure
#     schema = StructType([
#         StructField("key", StringType()),
#         StructField("value", StructType([
#             StructField("patient", StructType([
#                 StructField("personal_information", StructType([
#                     StructField("name", StringType()),
#                     StructField("age", IntegerType()),
#                     StructField("gender", StringType()),
#                     StructField("contact_details", StructType([
#                         StructField("phone", StringType()),
#                         StructField("address", StringType())
#                     ]))
#                 ])),
#                 StructField("medical_history", StructType([
#                     StructField("conditions", ArrayType(StringType())),
#                     StructField("allergies", ArrayType(StringType())),
#                     StructField("medications", ArrayType(StructType([
#                         StructField("name", StringType()),
#                         StructField("dosage", StringType()),
#                         StructField("frequency", StringType())
#                     ])))
#                 ])),
#                 StructField("appointments", ArrayType(StructType([
#                     StructField("date", StringType()),
#                     StructField("doctor", StringType()),
#                     StructField("reason", StringType())
#                 ])))
#             ]))
#         ]))
#     ])

#     # Create Kafka consumer instance
#     conf = {
#         'bootstrap.servers': 'host.docker.internal:9092',
#         'group.id': 'my_consumer_group_',
#         'auto.offset.reset': 'earliest'
#     }
#     consumer = Consumer(conf)
#     consumer.subscribe(['healthcare_patients'])

#     # Process the data and insert into MongoDB
#     while True:
#         msg = consumer.poll(timeout=1.0)
#         if msg is None:
#             continue
#         if msg.error():
#             if msg.error().code() == KafkaError._PARTITION_EOF:
#                 # End of partition event
#                 continue
#             else:
#                 # Error
#                 print("Consumer error:", msg.error())
#                 break
#         else:
#             json_data = json.loads(msg.value().decode('utf-8'))
#             print(json_data)

#             # Extracting the patient data
#             patient_data = json_data['patient']
            
#             # Convert to DataFrame
#             df = spark.createDataFrame([(json_data['key'], patient_data)], schema)

#             # Add timestamp column
#             df = df.withColumn("timestamp", current_timestamp())

#             # Write to MongoDB
#             df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append") \
#                 .option("uri", 'mongodb://root:root@host.docker.internal:27017/healthcare_db.spark_test').save()

#     consumer.close()


# if __name__ == "__main__":
#     consume_from_kafka_and_insert_to_mongodb()
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("CustomDeserializerMongoDB") \
    .config("spark.mongodb.output.uri", "mongodb://root:root@host.docker.internal:27017/healthcare_db.patients") \
    .getOrCreate()

# Sample JSON data
json_data = '{"name": "John Doe", "age": 35, "gender": "male"}'

# Deserialize JSON data and create RDD
rdd = spark.sparkContext.parallelize([json_data])

# Custom deserialization function
def deserialize_json(json_str):
    try:
        import json
        data = json.loads(json_str)
        return (data.get("name"), data.get("age"), data.get("gender"))
    except Exception as e:
        print("Error deserializing JSON:", e)
        return (None, None, None)

# Apply deserialization function and create DataFrame
deserialized_rdd = rdd.map(deserialize_json)
df = spark.createDataFrame(deserialized_rdd, ["name", "age", "gender"])

# Write DataFrame to MongoDB using your URI
df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("uri", "mongodb://root:root@host.docker.internal:27017/healthcare_db.patients").save()

# Stop the Spark session
spark.stop()
