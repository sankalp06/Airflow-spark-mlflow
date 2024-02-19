import mlflow

class ModelPromoter:
    def __init__(self, model_name):
        self.model_name = model_name

    def promote_to_stage(self, version, stage):
        try:
            client = mlflow.tracking.MlflowClient()
            client.transition_model_version_stage(
                name=self.model_name,
                version=version,
                stage=stage
            )
            print(f"Model version {version} promoted to stage: {stage}")
        except Exception as e:
            print(f"Error promoting model version {version} to stage {stage}: {e}")

    def promote_to_staging(self, version):
        self.promote_to_stage(version, "Staging")

    def promote_to_production(self, version):
        self.promote_to_stage(version, "Production")


# if __name__ == "__main__":
#     mlflow.set_tracking_uri('http://host.docker.internal:5000')
#     model_name = "Used_Car_price_prediction"
#     # model_promoter = ModelPromoter(model_name)

#     # version_to_promote = 3  
#     # model_promoter.promote_to_production(version_to_promote)

#     import mlflow.pyfunc

#  # Optional, if not specified, it loads the latest version
#     stage = "Production"  # Specify the stage you want to load

#     loaded_model = mlflow.sklearn.load_model(
#         model_uri=f"models:/{model_name}/{stage}"
#     )
    
