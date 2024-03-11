from pyspark.sql import SparkSession
import mlflow.pyfunc
from pyspark.sql.functions import struct, col

# Create a Spark session
spark = SparkSession.builder \
    .appName("MLflowPrediction") \
    .getOrCreate()

# Define the MLflow model URI
logged_model = 'runs:/1f89a0a069c3476993f3700e79bb9a94/model'

# Load model as a Spark UDF
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model)

# Sample DataFrame (replace it with your actual DataFrame)
data = [("Alice", 34, "female"), ("Bob", 45, "male")]
columns = ["name", "age", "gender"]
df = spark.createDataFrame(data, columns)

# Predict on the DataFrame
df_with_predictions = df.withColumn('predictions', loaded_model(struct(*map(col, df.columns))))

# Show the DataFrame with predictions
df_with_predictions.show()

# Stop the Spark session
spark.stop()
