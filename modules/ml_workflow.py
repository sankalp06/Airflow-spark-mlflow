from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import sys
# Append path to sys
add_path_to_sys = "/opt/airflow/"
sys.path.append(add_path_to_sys)

from utils.model_pipeline import ModelPipeline
from utils.model_evaluator import ModelEvaluator
from utils.experiment_tracking import ExperimentCreator
from plugins.S3_conn import MinIODataFrameHandler

class MLWorkflow(MinIODataFrameHandler, ModelPipeline, ModelEvaluator, ExperimentCreator):
    def __init__(self, minio_server_url, access_key, secret_key, source_object_key, source_bucket, target_feature, mlflow_tracking_uri, mlflow_experiment_name):
        super().__init__(minio_server_url=minio_server_url, access_key=access_key, secret_key=secret_key)
        self.minio_server_url = minio_server_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.source_object_key = source_object_key
        self.source_bucket = source_bucket
        self.target_feature = target_feature
        self.mlflow_tracking_uri = mlflow_tracking_uri
        self.mlflow_experiment_name = mlflow_experiment_name
        
    def execute_workflow(self):
        data = MinIODataFrameHandler(self.minio_server_url, self.access_key, self.secret_key)
        pipeline = ModelPipeline(target_column=self.target_feature)
        creator = ExperimentCreator(tracking_uri=self.mlflow_tracking_uri)
        df = data.download_dataframe(self.source_object_key, self.source_bucket)
        X, y = pipeline.separate_features_target(df)
        numerical_features, categorical_features = pipeline.identify_features(X)
        pipeline.create_preprocessor(numerical_features, categorical_features)
        regressor = RandomForestRegressor()
        pipeline.create_pipeline(regressor)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        model = pipeline.train_pipeline(X_train, y_train)
        evaluator = ModelEvaluator(model)
        performance_metrics = evaluator.evaluate_regression_model(X_test, y_test)
        creator.create_experiment(self.mlflow_experiment_name, performance_metrics, model)

