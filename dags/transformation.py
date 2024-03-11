from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import os
spark_job = "/opt/airflow/jobs" 
os.chdir(spark_job)


default_args = {
    'owner': 'sankalp',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('transformation', default_args=default_args, schedule_interval='@daily')

submit_spark_job = BashOperator(
    task_id='submit_spark_job',
    #bash_command='spark-submit --master spark://spark-master:7077 --name spark_job --deploy-mode client --packages org.mongodb.spark:mongo-spark-connector:10.0.2 /opt/airflow/jobs/tt.py',
    bash_command='spark-submit --master spark://spark-master:7077 --name spark_job --deploy-mode client --packages org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.375 /opt/airflow/jobs/in.py',
    dag=dag
)


# submit_spark_job1 = BashOperator(
#     task_id='submit_spark_job1',
#     bash_command='spark-submit --master spark://spark-master:7077 --name spark_job --deploy-mode client --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2 /opt/airflow/jobs/kafka_consumer.py',
#     #bash_command='spark-submit --master spark://spark-master:7077 --name spark_job --deploy-mode client --packages org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.375 /opt/airflow/jobs/t1.py',
#     dag=dag
# )

submit_spark_job # >> submit_spark_job1spark-submit --master spark://spark-master:7077 --name spark_job --deploy-mode client --packages /opt/bitnami/jobs/tt.py