from mlflow.tracking import MlflowClient
from collections import defaultdict
import mlflow

class ModelPerformanceComparator:
    def __init__(self, experiment_name, metric_name, threshold, comparison_operator):
        self.experiment_name = experiment_name
        self.metric_name = metric_name
        self.threshold = threshold
        self.comparison_operator = comparison_operator

    def find_best_model_run(self):
        # Create an MLflow client
        client = MlflowClient()

        # Get experiment ID
        experiment = client.get_experiment_by_name(self.experiment_name)
        if experiment:
            experiment_id = experiment.experiment_id
        else:
            print(f"Experiment '{self.experiment_name}' not found.")
            return None, None, None

        # Retrieve runs for the experiment
        runs = client.search_runs(experiment_ids=[experiment_id])

        # Dictionary to store the best model run for each model name
        best_runs = defaultdict(lambda: {"run_id": None, "metric_value": None})

        # Iterate through runs and find the best model run for each model name
        for run in runs:
            run_id = run.info.run_id
            model_name = run.data.tags['mlflow.runName']
            metric_value = run.data.metrics.get(self.metric_name)

            # Check if the metric value satisfies the condition based on the comparison operator
            if self.comparison_operator == "greater":
                if metric_value is not None and metric_value > self.threshold and (best_runs[model_name]["metric_value"] is None or metric_value > best_runs[model_name]["metric_value"]):
                    best_runs[model_name]["run_id"] = run_id
                    best_runs[model_name]["metric_value"] = metric_value
            elif self.comparison_operator == "less":
                if metric_value is not None and metric_value < self.threshold and (best_runs[model_name]["metric_value"] is None or metric_value < best_runs[model_name]["metric_value"]):
                    best_runs[model_name]["run_id"] = run_id
                    best_runs[model_name]["metric_value"] = metric_value

        # Find the model run with the highest metric value among the best runs
        if best_runs:
            best_model_run = max(best_runs.values(), key=lambda x: x["metric_value"])
            return best_model_run["run_id"], best_model_run["metric_value"], model_name
        else:
            print("No runs found that meet the criteria.")
            return None, None, None



# # Example usage:
# if __name__ == "__main__":
#     # Initialize ModelPerformanceComparator
#     mlflow.set_tracking_uri("http://host.docker.internal:5000")
#     comparator = ModelPerformanceComparator("Used_Car_price_prediction", "R2", 0.8, "greater")
#     # Find the best model run
#     best_run_id, best_metric_value, best_model_name = comparator.find_best_model_run()
#     if best_run_id:
#         print(f"Best run ID: {best_run_id}, Best metric value: {best_metric_value}, Model name: {best_model_name}")
#     else:
#         print("No runs found below the threshold.")

