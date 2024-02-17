
import mlflow
from datetime import datetime

class ExperimentCreator:
    def __init__(self, tracking_uri):
        self.tracking_uri = tracking_uri

    def create_experiment(self, experiment_name, performance_metrics, model_pipeline, run_params=None):
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(experiment_name)
        run_name = experiment_name + str(datetime.now().strftime("%d-%m-%y"))

        with mlflow.start_run(run_name=run_name):
            if run_params is not None:
                for param in run_params:
                    mlflow.log_param(param, run_params[param])

            for metric in performance_metrics:
                mlflow.log_metric(metric, performance_metrics[metric])

            mlflow.sklearn.log_model(model_pipeline, "model")
