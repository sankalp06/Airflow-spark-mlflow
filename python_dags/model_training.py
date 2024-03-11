from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sklearn.ensemble import RandomForestRegressor
import sys
import yaml

# Append path to sys
add_path_to_sys = "/opt/airflow/"
sys.path.append(add_path_to_sys)

from modules.ml_workflow import MLWorkflow

# Load DAG configurations from YAML file
with open("/opt/airflow/config/model_dev_dag_config.yaml", 'r') as stream:
    dag_config = yaml.safe_load(stream)

default_args = {
    'owner': 'sankalp',
    'start_date': datetime.utcnow(),
    'retries': 1
}

dag = DAG(
    'model_training',
    default_args=default_args,
    description='DAG for training and evaluating ML model',
    schedule_interval='@daily'
)

def ml_workflow(**kwargs):
    task_params = dag_config['model_params']
    # Use task_params
    minio_server_url = task_params['minio_server_url']
    access_key = task_params['access_key']
    secret_key = task_params['secret_key']
    source_object_key = task_params['source_object_key']
    source_bucket = task_params['source_bucket']
    target_feature = task_params['target_feature']
    mlflow_tracking_uri = task_params['mlflow_tracking_uri']
    mlflow_experiment_name = task_params['mlflow_experiment_name']
    ml_workflow = MLWorkflow(minio_server_url, access_key, secret_key, source_object_key, source_bucket, target_feature, mlflow_tracking_uri, mlflow_experiment_name)
    ml_workflow.execute_workflow()


# Define the PythonOperator to execute the test() function
train_model_task = PythonOperator(
    task_id='train_model_task',
    python_callable=ml_workflow,
    provide_context=True,
    op_kwargs=dag_config['model_params'],
    dag=dag
)

train_model_task

 