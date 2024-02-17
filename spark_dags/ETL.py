from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import os

spark_job = "/opt/airflow/jobs"
os.chdir(spark_job)

default_args = {
    'owner': 'sankalp',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('ETL', default_args=default_args, schedule_interval='@daily')

# Define tasks
extract_task = BashOperator(
    task_id='extract',
    bash_command='echo "Extracting data"',
    dag=dag
)


submit_spark_job = BashOperator(
    task_id='transform',
    bash_command='spark-submit --master spark://spark-master:7077 --name spark_job --deploy-mode client /opt/airflow/jobs/transformation.py',
    dag=dag
)

load_task = BashOperator(
    task_id='load',
    bash_command='echo "Loading data"',
    dag=dag
)
# Define task dependencies
extract_task >> submit_spark_job >> load_task
