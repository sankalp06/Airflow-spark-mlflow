import pandas as pd
import boto3
from io import StringIO

class MinIODataFrameHandler:
    def __init__(self, minio_server_url, access_key, secret_key):
        self.minio_server_url = minio_server_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.minio_client = boto3.client(
            's3',
            endpoint_url=minio_server_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    def upload_dataframe(self, df, destination_object_key,destination_bucket):
        try:
            # Convert DataFrame to CSV format in-memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload CSV data to the bucket
            self.minio_client.put_object(
                Bucket=destination_bucket,
                Key=destination_object_key,
                Body=csv_buffer.getvalue()
            )
            print(f"DataFrame uploaded successfully to MinIO bucket '{destination_bucket}' as '{destination_object_key}'")
            return True
        except Exception as e:
            print(f"Error uploading DataFrame to MinIO: {e}")
            return False

    def download_dataframe(self, source_object_key,source_bucket):
        try:
            # Download object from the bucket
            response = self.minio_client.get_object(
                Bucket=source_bucket,
                Key=source_object_key
            )

            # Read the object data as CSV and load it into a DataFrame
            csv_data = response['Body'].read().decode('latin-1')
            df = pd.read_csv(StringIO(csv_data))

            print(f"DataFrame downloaded successfully from MinIO bucket '{source_bucket}' as '{source_object_key}'")
            return df
        except Exception as e:
            print(f"Error downloading DataFrame from MinIO: {e}")
            return None
