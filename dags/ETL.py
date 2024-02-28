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

# transform and upload data
def process_and_upload_data(**kwargs):
    task_params = dag_config['task_params']
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
    op_kwargs=dag_config['task_params'],
    dag=dag,
)


# Dummy load task
load_task = DummyOperator(
    task_id='load',
    dag=dag,
)

# Define task dependencies
extract_task >> transformation_task >> load_task




