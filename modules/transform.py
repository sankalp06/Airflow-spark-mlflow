import sys
add_path_to_sys = "/opt/airflow/" 
sys.path.append(add_path_to_sys)

from plugins.S3_conn import MinIODataFrameHandler
from utils.transformations import DataFrameTransformer

class transformer(DataFrameTransformer, MinIODataFrameHandler):
    def __init__(self, minio_server_url, access_key, secret_key):
        # Initialize parent classes
        DataFrameTransformer.__init__(self)
        MinIODataFrameHandler.__init__(self, minio_server_url, access_key, secret_key)

    def process_data_and_upload(self, source_bucket, source_object_key, destination_bucket, destination_object_key,
                                column_types_to_convert, columns_to_remove, feature_engineering_transformations,
                                numerical_columns_list):
        # Step 1: Download data from the source bucket
        data_df = self.download_dataframe(source_object_key, source_bucket)
        if data_df is None:
            print("Error: Unable to download data from the source bucket.")
            return False

        # Step 2: Perform transformations
        self.convert_data_types(data_df,column_types_to_convert)
        self.apply_feature_engineering(data_df,feature_engineering_transformations)
        self.remove_unnecessary_columns(data_df,columns_to_remove)
        self.handle_outliers(data_df, numerical_columns_list)

        # Step 3: Upload processed data to the destination bucket
        success = self.upload_dataframe(data_df, destination_object_key, destination_bucket)
        if success:
            print("Data processed and uploaded successfully to the destination bucket.")
            return True
        else:
            print("Error: Failed to upload processed data to the destination bucket.")
            return False

