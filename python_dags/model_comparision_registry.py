from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import mlflow
import yaml

import sys
add_path_to_sys = "/opt/airflow/"
sys.path.append(add_path_to_sys)

from utils.model_performence_comparator import ModelPerformanceComparator
from utils.model_registry import ModelRegistry

with open('/opt/airflow/config/model_comparision_dag_config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Define the default arguments for the DAG
default_args = {
    'owner': 'sankalp',
    'start_date': datetime.utcnow(),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'model_comparison_and_registration',
    default_args=default_args,
    description='DAG for comparing model performance and registering the best model',
    schedule_interval= '@daily',
)

def compare_model_performance(**kwargs):
    experiment_name = config['experiment_name']
    metric_name = config['metric_name']
    threshold = config['threshold']
    comparison_operator = config['comparison_operator']
    mlflow_tracking_uri = config['mlflow_tracking_uri']
    # Create an instance of ModelPerformanceComparator
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    comparator = ModelPerformanceComparator(experiment_name, metric_name, threshold, comparison_operator)

    # Find the best model run
    best_run_id, best_metric_value, best_model_name = comparator.find_best_model_run()
    if best_run_id:
        print(f"Best run ID: {best_run_id}, Best metric value: {best_metric_value}, Model name: {best_model_name}")
        kwargs['ti'].xcom_push(key='best_run_id', value=best_run_id)
    else:
        print("No runs found below the threshold.")

def register_best_model(**kwargs):
    mlflow.set_tracking_uri("http://host.docker.internal:5000")
    best_run_id = kwargs['ti'].xcom_pull(task_ids='compare_model_performance', key='best_run_id')
    if best_run_id:
        model_registry = ModelRegistry()
        registered_model_name = config['registered_model_name']
        model_registry.register_model(best_run_id, registered_model_name)


compare_model_performance_task = PythonOperator(
    task_id='compare_model_performance',
    python_callable=compare_model_performance,
    dag = dag,
)

register_best_model_task = PythonOperator(
    task_id='register_best_model',
    python_callable=register_best_model,
    dag = dag,
)


compare_model_performance_task >> register_best_model_task
