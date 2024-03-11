import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import sys

# Append path to sys
add_path_to_sys = "/opt/airflow/"
sys.path.append(add_path_to_sys)

# Import the required function from the transform module
from modules.transform import transformer
from modules.extract.mongodb_S3 import MongoDBToS3CSVExporter  #MongoDBToMinIOCSVExporter
from modules.extract.postgres_S3 import PostgreSQLToMinIO

# Load DAG configurations from YAML file
with open("/opt/airflow/config/etl_dag_config.yaml", 'r') as stream:
    dag_config = yaml.safe_load(stream)

default_args = {
    'owner': 'sankalp',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
}

dag = DAG(
    'ETL',
    default_args=default_args,
    description='Extract transform load pipline',
    schedule_interval=timedelta(days=1),
)

def mongodb_to_S3():
    monogo_to_minio_config = dag_config['extract']['monogo_to_minio']
    mongodb_uri = monogo_to_minio_config['mongodb_uri']
    database_name = monogo_to_minio_config['database_name']
    collection_name = monogo_to_minio_config['collection_name']
    minio_endpoint = monogo_to_minio_config['minio_endpoint']
    minio_access_key = monogo_to_minio_config['minio_access_key']
    minio_secret_key = monogo_to_minio_config['minio_secret_key']
    minio_bucket_name = monogo_to_minio_config['minio_bucket_name']
    minio_object_key = monogo_to_minio_config['minio_object_key']
    
    exporter = MongoDBToS3CSVExporter(mongodb_uri, database_name, collection_name, minio_endpoint, minio_access_key, minio_secret_key, minio_bucket_name, minio_object_key )
    exporter.export_new_data_to_csv_in_s3()


def postgres_to_s3():
    # Extract PostgreSQL connection parameters
    config = dag_config['extract']['postgres_to_minio']
    pg_conn_params = config['pg_conn_params']
    # Extract MinIO parameters
    minio_endpoint = config['minio_endpoint']
    minio_access_key = config['minio_access_key']
    minio_secret_key = config['minio_secret_key']
    minio_bucket_name = config['minio_bucket_name']
    minio_object_key = config['minio_object_key']
    query = config['query']

    # Create an instance of PostgreSQLToMinIO
    pg_to_minio = PostgreSQLToMinIO(pg_conn_params, minio_endpoint, minio_access_key, minio_secret_key, minio_bucket_name, minio_object_key)
    # Extract data from PostgreSQL
    data = pg_to_minio.extract_data_from_postgres(query)

    if data:
        # Upload data to MinIO
        pg_to_minio.upload_data_to_minio(data)
    else:
        print("No data extracted from PostgreSQL.")



# transform and upload data
def process_and_upload_data(**kwargs):
    task_params = dag_config['transformation_params']
    # Use task_params
    minio_server_url = task_params['minio_server_url']
    access_key = task_params['access_key']
    secret_key = task_params['secret_key']
    source_bucket = task_params['source_bucket']
    destination_bucket = task_params['destination_bucket']
    source_object_key = task_params['source_object_key']
    destination_object_key = task_params['destination_object_key']
    column_types_to_convert = task_params['column_types_to_convert']
    columns_to_remove = task_params['columns_to_remove']
    feature_engineering_transformations = task_params['feature_engineering_transformations']
    numerical_columns_list = task_params['numerical_columns_list']

    # Initialize DataFrameMinIOHandler object
    handler = transformer(minio_server_url, access_key, secret_key)

    # Process data and upload to destination bucket
    handler.process_data_and_upload(source_bucket, source_object_key, destination_bucket, destination_object_key,
                                    column_types_to_convert, columns_to_remove, feature_engineering_transformations,
                                    numerical_columns_list)

mongo_to_s3 = PythonOperator(
    task_id='mongodb',
    python_callable=mongodb_to_S3,
    provide_context=True,
    #op_kwargs=dag_config['transformation_params'],
    dag=dag,
)

postgresdb_to_s3 = PythonOperator(
    task_id='postgresdb',
    python_callable=postgres_to_s3,
    provide_context=True,
    #op_kwargs=dag_config['transformation_params'],
    dag=dag,
)

# Dummy extract task
extract_task = DummyOperator(
    task_id='extract',
    dag=dag,
)

# Define the Python operator
transformation_task = PythonOperator(
    task_id='Transform',
    python_callable=process_and_upload_data,
    provide_context=True,
    #op_kwargs=dag_config['transformation_params'],
    dag=dag,
)


load_task = DummyOperator(
    task_id='load',
    dag=dag,
)


[postgresdb_to_s3, mongo_to_s3] >> extract_task >> transformation_task >> load_task

