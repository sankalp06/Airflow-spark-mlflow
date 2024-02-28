import psycopg2
import boto3
from io import StringIO
import csv

class PostgreSQLToMinIO:
    def __init__(self, pg_conn_params, minio_endpoint, minio_access_key, minio_secret_key, minio_bucket_name, minio_object_key):
        self.pg_conn_params = pg_conn_params
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket_name = minio_bucket_name
        self.minio_object_key = minio_object_key

    def extract_data_from_postgres(self, query):
        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(**self.pg_conn_params)
            cursor = conn.cursor()

            # Execute SQL query
            cursor.execute(query)

            # Fetch data
            data = cursor.fetchall()

            # Close connection
            cursor.close()
            conn.close()

            return data

        except Exception as e:
            print("Error extracting data from PostgreSQL:", str(e))
            return None

    def upload_data_to_minio(self, data):
        try:
            # Write data to CSV string
            csv_buffer = StringIO()
            csv_writer = csv.writer(csv_buffer)
            csv_writer.writerows(data)

            # Connect to MinIO and upload the CSV file
            s3 = boto3.client(
                's3',
                endpoint_url=self.minio_endpoint,
                aws_access_key_id=self.minio_access_key,
                aws_secret_access_key=self.minio_secret_key,
                region_name='',  # region is not needed for MinIO
                config=boto3.session.Config(signature_version='s3v4')
            )

            csv_buffer.seek(0)
            s3.put_object(Bucket=self.minio_bucket_name, Key=self.minio_object_key, Body=csv_buffer.getvalue())
            print("CSV file uploaded to MinIO successfully.")

        except Exception as e:
            print("Error uploading data to MinIO:", str(e))



