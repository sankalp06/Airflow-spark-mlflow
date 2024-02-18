from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
import yaml

import sys
add_path_to_sys = "/opt/airflow/"
sys.path.append(add_path_to_sys)

# Import required class

with open("/opt/airflow/config/etl_dag_config.yaml", 'r') as stream:
    dag_config = yaml.safe_load(stream)

from modules.inference import MLflowMinIOModelLoader

# Define the default arguments for the DAG
default_args = {
    'owner': 'sankalp',
    'start_date': datetime(2024, 2, 17),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'inference',
    default_args=default_args,
    description='Perform MLflow inference and store predictions in MinIO',
    schedule_interval= '@daily',
)


# Import the required function from the transform module
from modules.transform import transformer

# transform and upload data
def process_and_upload_data(**kwargs):
    task_params = dag_config['task_params']
     # Read parameters from the YAML file
    with open('/opt/airflow/config/inference_dag_config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    with open('/opt/airflow/config/inference_etl_config.yaml', 'r') as file:
        etl_config = yaml.safe_load(file)

    minio_server_url = etl_config['minio_server_url']
    access_key = etl_config['access_key']
    secret_key = etl_config['secret_key']
    source_bucket = etl_config['source_bucket']
    destination_bucket = etl_config['destination_bucket']
    source_object_key = etl_config['source_object_key']
    destination_object_key = etl_config['destination_object_key']
    column_types_to_convert = etl_config['column_types_to_convert']
    columns_to_remove = task_params['columns_to_remove']
    feature_engineering_transformations = task_params['feature_engineering_transformations']
    numerical_columns_list = etl_config['numerical_columns_list']

    # Initialize DataFrameMinIOHandler object
    handler = transformer(minio_server_url, access_key, secret_key)

    # Process data and upload to destination bucket
    handler.process_data_and_upload(source_bucket, source_object_key, destination_bucket, destination_object_key,
                                    column_types_to_convert, columns_to_remove, feature_engineering_transformations,
                                    numerical_columns_list)


# Define the PythonOperator task
def inference(**kwargs):
    # Read parameters from the YAML file
    with open('/opt/airflow/config/inference_dag_config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    import mlflow
    mlflow_tracking_uri = "http://host.docker.internal:5000"
    mlflow.set_tracking_uri(mlflow_tracking_uri)

    minio_server_url = config['minio_server_url']
    access_key = config['access_key']
    secret_key = config['secret_key']
    model_uri = config['model_uri']
    source_bucket = config['source_bucket']
    source_object_key = config['source_object_key']
    destination_bucket = config['destination_bucket']
    destination_object_key = config['destination_object_key']
    mlflow_tracking_uri = config['mlflow_tracking_uri']

    mlflow_minio_model_loader = MLflowMinIOModelLoader(
        minio_server_url,
        access_key,
        secret_key,
        model_uri
    )
    # Download DataFrame, make predictions, and upload DataFrame with predictions
    mlflow_minio_model_loader.download_predict_upload(
        source_object_key,
        source_bucket,
        destination_object_key,
        destination_bucket,
        mlflow_tracking_uri
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
    op_kwargs=dag_config['task_params'],
    dag=dag,
)


# Dummy load task
load_task = DummyOperator(
    task_id='load',
    dag=dag,
)



run_inference = PythonOperator(
    task_id='run_inference',
    python_callable=inference,
    dag=dag,
)


# Define task dependencies
extract_task >> transformation_task >> run_inference >> load_task


