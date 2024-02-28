import sys
add_path_to_sys = "/opt/airflow/"
sys.path.append(add_path_to_sys)

from modules.ingestion.kafka_producer import produce_to_kafka
from modules.ingestion.kafka_consumer import consume_from_kafka_and_insert_to_mongodb
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import yaml

def read_config_from_yaml(yaml_file):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config


default_args = {
    'owner': 'sankalp',
    'start_date': datetime.utcnow(),
    'retries': 1,
}

dag = DAG(
    'kafka_mongodb_ingestion_pipeline',
    default_args=default_args,
    description='A DAG to handle Kafka and MongoDB pipeline',
    schedule_interval=timedelta(days=1),
)

config = read_config_from_yaml('/opt/airflow/config/kafka_config.yaml')
consumer_config = config['consumer_config']
producer_config = config['producer_config']

def kafka_producer_task():
    bootstrap_servers = 'host.docker.internal:9092'
    produce_to_kafka(bootstrap_servers,
        producer_config['json_file'],
        producer_config['topic']
    )

producer_task = PythonOperator(
    task_id='kafka_producer_task',
    python_callable=kafka_producer_task,
    dag=dag,
)

def kafka_consumer_task():
    bootstrap_servers = 'host.docker.internal:9092'
    consume_from_kafka_and_insert_to_mongodb(bootstrap_servers,
        consumer_config['topic'],
        consumer_config['group_id'],
        consumer_config['mongo_uri'],
        consumer_config['mongo_db_name'],
        consumer_config['mongo_collection_name'],
        consumer_config['message_limit'],
        consumer_config['batch_size']
    )

consumer_task = PythonOperator(
    task_id='kafka_consumer_task',
    python_callable=kafka_consumer_task,
    dag=dag,
)

producer_task >> consumer_task
