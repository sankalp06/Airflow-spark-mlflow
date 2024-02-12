from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime, timedelta
import pandas as pd
import boto3
from io import StringIO

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to convert DataFrame to CSV and upload to MinIO
def upload_to_minio():
    # Sample DataFrame
    data = {
        'Name': ['John', 'Alice', 'Bob'],
        'Age': [30, 25, 40],
        'City': ['New York', 'Los Angeles', 'Chicago']
    }
    df = pd.DataFrame(data)

    # Convert DataFrame to CSV format in-memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Configure the MinIO endpoint and credentials
    minio_client = boto3.client(
        's3',
        endpoint_url='http://host.docker.internal:9000',
        aws_access_key_id='NWLUIMTyWmDpvc7rDTYe',
        aws_secret_access_key='KX6vXLk0dz42NkBRgsV5gRIpmVYlyOfpg6joowzS',
    )

    # Define the bucket name
    bucket_name = 'bronze-data'

    # Upload CSV data to the bucket
    try:
        response = minio_client.put_object(
            Bucket=bucket_name,
            Key='dataa.csv',
            Body=csv_buffer.getvalue()
        )
        print("CSV data uploaded successfully")
    except Exception as e:
        print(f"Error uploading CSV data: {e}")

# Define the DAG
dag = DAG(
    'upload_to_minio',
    default_args=default_args,
    description='Upload DataFrame to MinIO',
    schedule_interval=None,
)

# Define the task
upload_task = PythonOperator(
    task_id='upload_to_minio_task',
    python_callable=upload_to_minio,
    dag=dag,
)

# Define task dependencies (if any)
# upload_task.set_upstream(...)

if __name__ == "__main__":
    dag.cli()
