from pyspark.sql import SparkSession, functions as F, Window
from io import StringIO
import pandas as pd
import boto3


class MinIODataFrameHandler:
    def __init__(self, minio_server_url, access_key, secret_key):
        self.minio_server_url = minio_server_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.minio_client = boto3.client(
            's3',
            endpoint_url=minio_server_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    def upload_dataframe(self, df, object_key,bucket_name):
        try:
            # Convert DataFrame to CSV format in-memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload CSV data to the bucket
            self.minio_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=csv_buffer.getvalue()
            )
            print(f"DataFrame uploaded successfully to MinIO bucket '{self.bucket_name}' as '{object_key}'")
            return True
        except Exception as e:
            print(f"Error uploading DataFrame to MinIO: {e}")
            return False

    def download_dataframe(self, object_key, bucket_name):
        try:
            # Download object from the bucket
            response = self.minio_client.get_object(
                Bucket=bucket_name,
                Key=object_key
            )
            # Read the object data as CSV
            csv_data = response['Body'].read().decode('utf-8')

            # Inference the schema using PySpark
            spark = SparkSession.builder \
                .appName("Schema Inference") \
                .getOrCreate()
            # Create a Spark DataFrame for schema inference
            df_spark = spark.read.option("header", "true").option("inferSchema", "true").csv(StringIO(csv_data))

            print(f"DataFrame downloaded successfully from MinIO bucket '{bucket_name}' as '{object_key}'")
            return df_spark
        except Exception as e:
            print(f"Error downloading DataFrame from MinIO: {e}")
            return None


class AnotherClass:
    def __init__(self, minio_handler):
        self.minio_handler = minio_handler

    def download_from_bucket(self, source_object_key,bucket_name):
        # Download DataFrame from the source bucket
        df = self.minio_handler.download_dataframe(source_object_key,bucket_name)
        return df

    def upload_to_bucket(self, df, destination_object_key,bucket_name):
        # Upload DataFrame to the destination bucket
        if df is not None:
            self.minio_handler.upload_dataframe(df, destination_object_key,bucket_name)
        else:
            print("Dataframe is empty, cannot upload.")

    def convert_data_types(self, df, column_types=None):
        if column_types:
            for column, data_type in column_types.items():
                df = df.withColumn(column, F.col(column).cast(data_type))
        return df

    def apply_feature_engineering(self, df, transformations=None):
        if transformations:
            for new_column, expression in transformations.items():
                df = df.withColumn(new_column, F.expr(expression))
        return df

    def remove_unnecessary_columns(self, df, columns_to_remove=None):
        if columns_to_remove:
            df = df.drop(*columns_to_remove)
        return df

    def handle_outliers(self, df, numerical_columns=None, lower_bound=None, upper_bound=None):
        if numerical_columns:
            for column in numerical_columns:
                if lower_bound is None or upper_bound is None:
                    # If bounds are not provided, calculate bounds based on IQR
                    quartiles = df.approxQuantile(column, [0.25, 0.75], 0.05)
                    iqr = quartiles[1] - quartiles[0]
                    lower_bound = quartiles[0] - 1.5 * iqr
                    upper_bound = quartiles[1] + 1.5 * iqr

                # If lower_bound and upper_bound are provided, use winsorizing
                lower_win = F.lag(F.col(column)).over(Window.orderBy(F.col(column)))
                upper_win = F.lead(F.col(column)).over(Window.orderBy(F.col(column)))

                # Replace values outside the bounds with the nearest non-extreme values
                df = df.withColumn(column,
                                   F.when((F.col(column) < lower_bound) | (F.col(column) > upper_bound),
                                          F.coalesce(lower_win, upper_win, F.col(column))))
        return df


column_types_to_convert = {
  "Price": "double",
  "Age_08_04": "int",
  "Mfg_Month": "int",
  "Mfg_Year": "int",
  "KM": "int",
  "Fuel_Type": "string",
  "HP": "int",
  "Met_Color": "string",
  "Color": "string",
  "Automatic": "string",
  "CC": "int",
  "Doors": "int",
  "Gears": "int",
  "Quarterly_Tax": "int",
  "Weight": "int",
  "Mfr_Guarantee": "string",
  "BOVAG_Guarantee": "string",
  "Guarantee_Period": "int",
  "ABS": "string",
  "Airbag_1": "string",
  "Airbag_2": "string",
  "Airco": "string",
  "Automatic_airco": "string",
  "Boardcomputer": "string",
  "CD_Player": "string",
  "Central_Lock": "string",
  "Powered_Windows": "string",
  "Power_Steering": "string",
  "Radio": "string",
  "Mistlamps": "string",
  "Sport_Model": "string",
  "Backseat_Divider": "string",
  "Metallic_Rim": "string",
  "Radio_cassette": "string",
  "Tow_Bar": "string"
}

# Remove unnecessary columns
columns_to_remove = ['Tow_Bar','Parking_Assistant','Cylinders','Model']

# Specify feature engineering transformations
feature_engineering_transformations = {
    'Total_Distance': 'Age_08_04 * KM',
    'Car_Age': 'year(current_date()) - Mfg_Year',
    'Mileage': 'KM / Car_Age'
}

numerical_columns_list = ['Age_08_04', 'KM', 'HP', 'CC', 'Doors', 'Gears', 'Quarterly_Tax','Weight','Guarantee_Period', 'Mfg_Year','Price']
# MinIO server configuration
minio_server_url = 'http://host.docker.internal:9000'
access_key = 'NWLUIMTyWmDpvc7rDTYe'
secret_key = 'KX6vXLk0dz42NkBRgsV5gRIpmVYlyOfpg6joowzS'
source_bucket = 'bronze-data'
destination_bucket = 'silver-data'
source_object_key = 'dataa.csv'
destination_object_key = 'processed_data.csv'

# Create MinIODataFrameHandler object
minio_handler = MinIODataFrameHandler(minio_server_url, access_key, secret_key)

# Create AnotherClass instance with MinIODataFrameHandler
another_class_instance = AnotherClass(minio_handler)

# Download data from source bucket
df_spark = another_class_instance.download_from_bucket(source_object_key,source_bucket)

# Perform data transformations
df = another_class_instance.convert_data_types(df_spark, column_types_to_convert)
df = another_class_instance.apply_feature_engineering(df, feature_engineering_transformations)
df = another_class_instance.remove_unnecessary_columns(df, columns_to_remove)

# Handling outliers
df = another_class_instance.handle_outliers(df, numerical_columns_list)

# Upload data to destination bucket
another_class_instance.upload_to_bucket(df, destination_object_key,destination_bucket)
