from utils.model_loader import MLflowModelLoader
from plugins.S3_conn import MinIODataFrameHandler
import mlflow

class MLflowMinIOModelLoader(MinIODataFrameHandler, MLflowModelLoader):
    def __init__(self, minio_server_url, access_key, secret_key, model_uri):
        MinIODataFrameHandler.__init__(self, minio_server_url, access_key, secret_key)
        MLflowModelLoader.__init__(self, model_uri)
        # Set MLflow tracking URI

    def download_predict_upload(self, source_object_key, source_bucket, destination_object_key, destination_bucket,mlflow_tracking_uri):
        # Download DataFrame from MinIO
        df = self.download_dataframe(source_object_key, source_bucket)
        mlflow.set_tracking_uri(mlflow_tracking_uri)
        
        if df is not None:
            # Predict on DataFrame
            predictions = self.predict_pandas_dataframe(df)
            
            # Combine original DataFrame with predictions
            df['predictions'] = predictions
            
            # Upload DataFrame with predictions to MinIO
            success = self.upload_dataframe(df, destination_object_key, destination_bucket)
            
            if success:
                print("DataFrame with predictions uploaded successfully.")
            else:
                print("Error uploading DataFrame with predictions.")
        else:
            print("Error downloading DataFrame. Prediction aborted.")
