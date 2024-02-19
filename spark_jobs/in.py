from pyspark.sql import SparkSession
import mlflow.spark
import mlflow
# Initialize Spark session
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

mlflow.set_tracking_uri("http://host.docker.internal:5000")
logged_model = 'runs:/f68d030078dc45b4a28d531e9b0fc4ab/transformation'

# Load model
loaded_model = mlflow.spark.load_model(logged_model)


# # Load model as a PyFuncModel.
# loaded_model = mlflow.pyfunc.load_model(logged_model)