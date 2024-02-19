from datetime import datetime
from pyspark.sql import SparkSession
import pandas as pd
import mlflow
import mlflow.spark
import sys
mlflow_tracking_uri = "http://host.docker.internal:5000"

add_path_to_sys = "/opt/airflow/scripts" 
sys.path.append(add_path_to_sys)
from minio_conn import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Model Inference") \
    .getOrCreate()

def pandas_to_spark(df):
    return spark.createDataFrame(df)

def apply_transformation_model(data, transformation_model):
    return transformation_model.transform(data)

def apply_inference_model(data, inference_model):
    return inference_model.transform(data)

def load_model_with_error_handling(model_run_id, model_type):
    try:
        model = mlflow.spark.load_model(f'runs:/{model_run_id}')
        print(f"Successfully loaded {model_type} model.")
        return model
    except Exception as e:
        print(f"Error loading {model_type} model: {e}")
        # Handle error as needed
        raise e

def main(transformation_pipeline_run_id, ml_pipeline_run_id,minio_server_url,access_key,secret_key,bucket_name):

    try:
        # Read data
         # Create MinIODataFrameHandler object
        minio_handler = MinIODataFrameHandler(minio_server_url, access_key, secret_key, bucket_name)
        df_pd = minio_handler.download_dataframe(object_key,bucket_name)

        # Convert pandas DataFrame to Spark DataFrame
        df_spark = pandas_to_spark(df_pd)

        # Load transformation model
        transformation_model = load_model_with_error_handling(transformation_pipeline_run_id, "transformation")

        # Apply transformation
        transformed_data = apply_transformation_model(df_spark, transformation_model)

        # Load inference model
        inference_model = load_model_with_error_handling(ml_pipeline_run_id, "inference")

        # Apply inference
        predictions = apply_inference_model(transformed_data, inference_model)
        predictions.show(3)
        # Upload DataFrame to MinIO
        pandas_df = predictions.toPandas()
        #minio_handler.upload_dataframe(pandas_df, object_key)

        # # Write predictions to Parquet
        # current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # predictions.write.parquet(f"/FileStore/tables/predictions_{current_datetime}")
        print("Predictions written successfully.")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == "__main__":
    transformation_pipeline_run_id = 'ca40cdb1ff2c4422a15cdaf1306f1b50/transformation'
    ml_pipeline_run_id = 'ca40cdb1ff2c4422a15cdaf1306f1b50/model'

    # MinIO server configuration
    minio_server_url = 'http://host.docker.internal:9000'
    access_key = 'NWLUIMTyWmDpvc7rDTYe'
    secret_key = 'KX6vXLk0dz42NkBRgsV5gRIpmVYlyOfpg6joowzS'
    bucket_name = 'inference-data'
    object_key = 'transformed_data.csv'

   
    # Call the main function with provided arguments
    main(transformation_pipeline_run_id, ml_pipeline_run_id,minio_server_url,access_key,secret_key,bucket_name)