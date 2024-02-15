import pandas as pd
import boto3
from io import StringIO


class MinIODataFrameHandler:
    def __init__(self, minio_server_url, access_key, secret_key, bucket_name):
        self.minio_server_url = minio_server_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        self.minio_client = boto3.client(
            's3',
            endpoint_url=minio_server_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    def upload_dataframe(self, df, object_key,bucket_name):
        try:
            # Convert DataFrame to CSV format in-memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload CSV data to the bucket
            self.minio_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=csv_buffer.getvalue()
            )
            print(f"DataFrame uploaded successfully to MinIO bucket '{self.bucket_name}' as '{object_key}'")
            return True
        except Exception as e:
            print(f"Error uploading DataFrame to MinIO: {e}")
            return False

    def download_dataframe(self, object_key,bucket_name):
        try:
            # Download object from the bucket
            response = self.minio_client.get_object(
                Bucket=self.bucket_name,
                Key=object_key
            )

            # Read the object data as CSV and load it into a DataFrame
            csv_data = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_data))

            print(f"DataFrame downloaded successfully from MinIO bucket '{self.bucket_name}' as '{object_key}'")
            return df
        except Exception as e:
            print(f"Error downloading DataFrame from MinIO: {e}")
            return None

# # Example usage
# if __name__ == "__main__":
#     # MinIO server configuration
#     minio_server_url = 'http://host.docker.internal:9000'
#     access_key = 'NWLUIMTyWmDpvc7rDTYe'
#     secret_key = 'KX6vXLk0dz42NkBRgsV5gRIpmVYlyOfpg6joowzS'
#     bucket_name = 'bronze-data'
#     object_key = 'raw_data.csv'

#     # Create MinIODataFrameHandler object
#     minio_handler = MinIODataFrameHandler(minio_server_url, access_key, secret_key, bucket_name)
#     df = pd.read_csv("/opt/airflow/data/ToyotaCorollaa.csv")

#     # Upload DataFrame to MinIO
#     minio_handler.upload_dataframe(df, object_key)

#     # Download DataFrame from MinIO
#     downloaded_df = minio_handler.download_dataframe(object_key)

#     # Display DataFrame
#     if downloaded_df is not None:
#         print(downloaded_df)
#     else:
#         print("DataFrame could not be downloaded.")
