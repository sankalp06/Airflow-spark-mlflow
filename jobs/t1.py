from pyspark.sql import SparkSession

# Create SparkSession with Maven dependencies
spark = SparkSession.builder \
    .appName("test spark") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "NWLUIMTyWmDpvc7rDTYe") \
    .config("spark.hadoop.fs.s3a.secret.key", "KX6vXLk0dz42NkBRgsV5gRIpmVYlyOfpg6joowzS") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()


# Read CSV file from MinIO
df = spark.read.csv("s3a://bronze-data/used_petrol_cars.csv")

df.show(2)
df.write.parquet("s3a://bronze-data/used_petrol_cars_parquet")


# spark-submit --packages com.amazonaws:aws-java-sdk-bundle:1.11.375,org.apache.hadoop:hadoop-aws:3.2.0 t1.py
