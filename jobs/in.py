from pyspark.sql import SparkSession
import mlflow.spark
from pyspark.sql.functions import struct, col
import mlflow
# Initialize Spark session
# Create SparkSession with Maven dependencies
spark = SparkSession.builder \
    .appName("Read from MinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "NWLUIMTyWmDpvc7rDTYe") \
    .config("spark.hadoop.fs.s3a.secret.key", "KX6vXLk0dz42NkBRgsV5gRIpmVYlyOfpg6joowzS") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

mlflow.set_tracking_uri("http://host.docker.internal:5000")


logged_model = 'runs:/1f89a0a069c3476993f3700e79bb9a94/model'

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model_ = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')


# Print the loaded model object
print("Loaded model:", loaded_model_)

df = spark.read.csv("s3a://inference-data/processed_inference_data.csv")
df.show(1)

import mlflow.pyfunc
import mlflow.tracking


# Get the artifact URI of the model
client = mlflow.tracking.MlflowClient()
run = client.get_run('1f89a0a069c3476993f3700e79bb9a94')
artifact_uri = run.info.artifact_uri

# Print the artifact URI
print("Artifact URI:", artifact_uri)
