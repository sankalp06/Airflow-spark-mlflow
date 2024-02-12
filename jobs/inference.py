from datetime import datetime
from pyspark.sql import SparkSession
import pandas as pd
import mlflow
import mlflow.spark

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

def main(inference_data_path, transformation_pipeline_run_id, ml_pipeline_run_id):
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Model Inference") \
        .getOrCreate()

    try:
        # Read data
        df_pd = pd.read_csv(inference_data_path)
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

    # Extract arguments
    inference_data_path = 'path'
    transformation_pipeline_run_id = '98929509b8d1421389d7623e679c1e44/transformation'
    ml_pipeline_run_id = '98929509b8d1421389d7623e679c1e44/model'

    # Call the main function with provided arguments
    main(inference_data_path, transformation_pipeline_run_id, ml_pipeline_run_id)