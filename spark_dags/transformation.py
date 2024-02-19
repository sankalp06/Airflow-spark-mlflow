from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import os
spark_job = "/opt/airflow/jobs" 
os.chdir(spark_job)


default_args = {
    'owner': 'sankalp',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('transformations', default_args=default_args, schedule_interval='@daily')

submit_spark_job = BashOperator(
    task_id='submit_spark_job',
    bash_command='spark-submit --master spark://spark-master:7077 --name spark_job --deploy-mode client /opt/airflow/jobs/t.py',
    dag=dag
)

submit_spark_job
