from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import os 
import sys

add_path_to_sys = "/opt/airflow/scripts" 
sys.path.append(add_path_to_sys)
dir_path = "/opt/airflow/data" 
os.chdir(dir_path)

from minio_conn import * 

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("transformations") \
    .getOrCreate()

class DataFrameTransformer:
    def __init__(self, file_location, file_type="csv", infer_schema="true", first_row_is_header="true", delimiter=","):
        self.file_location = file_location
        self.file_type = file_type
        self.infer_schema = infer_schema
        self.first_row_is_header = first_row_is_header
        self.delimiter = delimiter
        self.output_path = None
        self.spark = SparkSession.builder.appName("DataFrameTransformation").getOrCreate()
        self.df = None

    def read_data(self):
        self.df = self.spark.read.format(self.file_type) \
            .option("inferSchema", self.infer_schema) \
            .option("header", self.first_row_is_header) \
            .option("sep", self.delimiter) \
            .load(self.file_location)
        
    def read_csv(self):
        # Read CSV file using pandas
        df_pd = pd.read_csv(self.file_location, delimiter=self.delimiter)
        # Convert pandas DataFrame to Spark DataFrame
        self.df = self.spark.createDataFrame(df_pd)

    def read_delta_file(self, delta_location):
        self.df = self.spark.read.format("delta").load(delta_location)

    def convert_data_types(self, column_types=None):
        if column_types:
            for column, data_type in column_types.items():
                self.df = self.df.withColumn(column, F.col(column).cast(data_type))

    def apply_feature_engineering(self, transformations=None):
        if transformations:
            for new_column, expression in transformations.items():
                self.df = self.df.withColumn(new_column, F.expr(expression))

    def remove_unnecessary_columns(self, columns_to_remove=None):
        if columns_to_remove:
            self.df = self.df.drop(*columns_to_remove)

    def handle_outliers(self, numerical_columns=None, lower_bound=None, upper_bound=None):
        if numerical_columns:
            for column in numerical_columns:
                if lower_bound is None or upper_bound is None:
                    # If bounds are not provided, calculate bounds based on IQR
                    quartiles = self.df.approxQuantile(column, [0.25, 0.75], 0.05)
                    iqr = quartiles[1] - quartiles[0]
                    lower_bound = quartiles[0] - 1.5 * iqr
                    upper_bound = quartiles[1] + 1.5 * iqr

                # If lower_bound and upper_bound are provided, use winsorizing
                lower_win = F.lag(F.col(column)).over(Window.orderBy(F.col(column)))
                upper_win = F.lead(F.col(column)).over(Window.orderBy(F.col(column)))

                # Replace values outside the bounds with the nearest non-extreme values
                self.df = self.df.withColumn(column,
                                             F.when((F.col(column) < lower_bound) | (F.col(column) > upper_bound),
                                                    F.coalesce(lower_win, upper_win, F.col(column))))
        return self.df

    def save_to_parquet(self, output_path, mode):
        self.df.write.mode(mode).parquet(output_path)


    def save_to_delta(self, output_path, mode="overwrite"):
        # Save to Delta table with mergeSchema option
        self.df.write.option("mergeSchema", "true").mode(mode).format("delta").save(output_path)



column_types_to_convert = {
  "Model": "string",
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
columns_to_remove = ['Tow_Bar','Parking_Assistant','Cylinders']

# Specify feature engineering transformations
feature_engineering_transformations = {
    'Total_Distance': 'Age_08_04 * KM',
    'Car_Age': 'year(current_date()) - Mfg_Year',
    'Mileage': 'KM / Car_Age'
}

numerical_columns_list = ['Age_08_04', 'KM', 'HP', 'CC', 'Doors', 'Gears', 'Quarterly_Tax','Weight','Guarantee_Period', 'Mfg_Year','Price']
data_path = "/opt/airflow/data/ToyotaCorollaa.csv"
transformer = DataFrameTransformer(
    file_location=data_path,
    file_type="csv",
    first_row_is_header="true",
    delimiter=","
)

# MinIO server configuration
minio_server_url = 'http://host.docker.internal:9000'
access_key = 'NWLUIMTyWmDpvc7rDTYe'
secret_key = 'KX6vXLk0dz42NkBRgsV5gRIpmVYlyOfpg6joowzS'
bucket_name = 'silver-data'
object_key = 'transformed_data.csv'

# Read data
transformer.read_csv()

# Convert data types
transformer.convert_data_types(column_types=column_types_to_convert)

#Apply feature engineering transformations
transformer.apply_feature_engineering(transformations=feature_engineering_transformations)

transformer.remove_unnecessary_columns(columns_to_remove=columns_to_remove)

# Handle outliers with winsorizing
lower_bound = 10  # Set your lower bound
upper_bound = 90  # Set your upper bound
#transformer.handle_outliers(numerical_columns=list(column_types_to_convert.keys()), lower_bound=lower_bound, upper_bound=upper_bound)
transformed_data = transformer.handle_outliers(numerical_columns=numerical_columns_list)
transformed_data.show(1)
pandas_df = transformed_data.toPandas()



# Create MinIODataFrameHandler object
minio_handler = MinIODataFrameHandler(minio_server_url, access_key, secret_key, bucket_name)

# Upload DataFrame to MinIO
minio_handler.upload_dataframe(pandas_df, object_key)

# output_path_d = "/FileStore/tables/ToyotaCorolla_transformed_delta"

# transformer.save_to_delta(output_path=output_path_d, mode="overwrite")