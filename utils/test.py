import csv
import datetime
from io import StringIO
import boto3
from pymongo import MongoClient

class MongoDBToS3CSVExporter:
    def __init__(self, mongodb_uri, database_name, collection_name, minio_endpoint, minio_access_key, minio_secret_key, minio_bucket_name, minio_object_key):
        self.mongodb_uri = mongodb_uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.minio_endpoint = minio_endpoint
        self.s3_bucket_name = minio_bucket_name
        self.s3_object_key = minio_object_key
        self.aws_access_key_id = minio_access_key
        self.aws_secret_access_key = minio_secret_key

    
    def export_new_data_to_csv_in_s3(self):
        # Get the latest timestamp from MongoDB
        latest_timestamp_mongodb = self.get_latest_timestamp_from_mongodb()
        print("Latest timestamp from MongoDB:", latest_timestamp_mongodb)

        # Get the latest timestamp from the existing CSV file in S3
        latest_timestamp_s3 = self.get_latest_timestamp_from_s3()
        print("Latest timestamp from S3:", latest_timestamp_s3)

        # Compare timestamps
        if latest_timestamp_mongodb is None:
            print("Error: Unable to retrieve the latest timestamp from MongoDB.")
            return

        if latest_timestamp_s3 is not None and latest_timestamp_s3 >= latest_timestamp_mongodb:
            print("Data is up to date. No need to export.")
            return

        # Connect to MongoDB
        client = MongoClient(self.mongodb_uri)
        db = client[self.database_name]
        collection = db[self.collection_name]

        # Query MongoDB for records with a timestamp greater than the latest timestamp in S3
        new_records_mongodb = self.query_mongodb_for_new_records(latest_timestamp_s3)
        print("New records fetched from MongoDB:", new_records_mongodb)

        # If there are new records in MongoDB, append them to the existing CSV file in S3
        if new_records_mongodb:
            updated_csv_data = self.append_records_to_existing_csv_in_s3(new_records_mongodb)
            print("Updated CSV data:", updated_csv_data)
            self.upload_to_s3(updated_csv_data)

    def get_latest_timestamp_from_mongodb(self):
        try:
            client = MongoClient(self.mongodb_uri)
            db = client[self.database_name]
            collection = db[self.collection_name]

            # Find the document with the latest timestamp
            latest_document = collection.find_one({}, sort=[('timestamp', -1)])

            if latest_document:
                return latest_document.get('timestamp')

            # If no documents found, return current time as a fallback
            return datetime.datetime.now()

        except Exception as e:
            print(f"Error retrieving latest timestamp from MongoDB: {e}")
            return None

    def get_latest_timestamp_from_s3(self):
        try:
            s3 = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                endpoint_url=self.minio_endpoint,

            )

            try:
                # Check if the S3 object exists
                response = s3.head_object(Bucket=self.s3_bucket_name, Key=self.s3_object_key)
            except s3.exceptions.ClientError as e:
                # If the object does not exist, return None
                if e.response['Error']['Code'] == '404':
                    print("S3 object does not exist. Creating a new object.")
                    return None
                else:
                    # Handle other errors
                    raise e

            # If the object exists, proceed to read the timestamp
            csv_data = response['Body'].read().decode('utf-8').splitlines()
            reader = csv.DictReader(csv_data)
            latest_timestamp = None

            for row in reader:
                timestamp = row.get('timestamp')
                if timestamp and (latest_timestamp is None or timestamp > latest_timestamp):
                    latest_timestamp = timestamp
                    print("Timestamp found in CSV:", latest_timestamp)

            return latest_timestamp

        except Exception as e:
            print(f"Error retrieving CSV from S3: {e}")
            return None



    def query_mongodb_for_new_records(self, latest_timestamp_s3):
        try:
            # Connect to MongoDB
            client = MongoClient(self.mongodb_uri)
            db = client[self.database_name]
            collection = db[self.collection_name]

            # Construct MongoDB query based on latest S3 timestamp
            query = {}
            if latest_timestamp_s3:
                query["timestamp"] = {"$gt": latest_timestamp_s3}

            # Execute MongoDB query
            new_records = list(collection.find(query))
            return new_records

        except Exception as e:
            print(f"Error querying MongoDB for new records: {e}")
            return []


    def append_records_to_existing_csv_in_s3(self, new_records):
        try:
            csv_data = StringIO()
            csv_writer = csv.DictWriter(csv_data, fieldnames=['patient_name', 'patient_age', 'patient_gender', 'patient_phone', 'patient_address', 'conditions', 'allergies', 'medications_name', 'medications_dosage', 'medications_frequency', 'appointments_date', 'appointments_doctor', 'appointments_reason', 'timestamp'])
            csv_writer.writeheader()

            for record in new_records:
                patient_info = record.get('patient', {}).get('personal_information', {})
                medical_history = record.get('patient', {}).get('medical_history', {})
                appointments = record.get('patient', {}).get('appointments', [{}])
                contact_details = patient_info.get('contact_details', {})

                csv_writer.writerow({
                    'patient_name': patient_info.get('name', ''),
                    'patient_age': patient_info.get('age', ''),
                    'patient_gender': patient_info.get('gender', ''),
                    'patient_phone': contact_details.get('phone', ''), 
                    'patient_address': contact_details.get('address', ''), 
                    'conditions': ', '.join(medical_history.get('conditions', [])),
                    'allergies': ', '.join(medical_history.get('allergies', [])),
                    'medications_name': ', '.join([med['name'] for med in medical_history.get('medications', [])]),
                    'medications_dosage': ', '.join([med['dosage'] for med in medical_history.get('medications', [])]),
                    'medications_frequency': ', '.join([med['frequency'] for med in medical_history.get('medications', [])]),
                    'appointments_date': appointments[0].get('date', '') if appointments else '',
                    'appointments_doctor': appointments[0].get('doctor', '') if appointments else '',
                    'appointments_reason': appointments[0].get('reason', '') if appointments else '',
                    'timestamp': record.get('timestamp', '')
                })

            csv_data_str = csv_data.getvalue()
            print("CSV data to append:", csv_data_str)
            return csv_data_str

        except Exception as e:
            print(f"Error appending records to CSV: {e}")
            return None

    def upload_to_s3(self, csv_data):
        try:
            s3 = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                endpoint_url=self.minio_endpoint,
            )

            s3.put_object(
                Bucket=self.s3_bucket_name,
                Key=self.s3_object_key,
                Body=csv_data.encode('utf-8'),
                ContentType='text/csv'
            )

            print("CSV file uploaded to S3 successfully.")

        except Exception as e:
            print(f"Error uploading CSV to S3: {e}")




if __name__ == "__main__":
    exporter = MongoDBToS3CSVExporter(
        mongodb_uri = "mongodb://root:root@host.docker.internal:27017/",
        database_name = "healthcare_db",
        collection_name= "patients",
        minio_endpoint ="http://host.docker.internal:9000",
        minio_access_key= "NWLUIMTyWmDpvc7rDTYe",
        minio_secret_key= "KX6vXLk0dz42NkBRgsV5gRIpmVYlyOfpg6joowzS",
        minio_bucket_name= "bronze-data",
        minio_object_key="patients_data.csv"
    )
    exporter.export_new_data_to_csv_in_s3()


# if __name__ == "__main__":
   
#    get_latest_timestamp_from_s3(
#         minio_endpoint ="http://host.docker.internal:9000",
#         minio_access_key= "NWLUIMTyWmDpvc7rDTYe",
#         minio_secret_key= "KX6vXLk0dz42NkBRgsV5gRIpmVYlyOfpg6joowzS",
#         minio_bucket_name= "bronze-data",
#         minio_object_key="test.csv"
#     )


