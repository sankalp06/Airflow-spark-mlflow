import mlflow

class ModelPromoter:
    def __init__(self, model_name):
        self.model_name = model_name

    def promote_to_stage(self, version, stage):
        try:
            mlflow.registered_model.transition_model_version_stage(
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


# Example usage:
if __name__ == "__main__":
    model_name = "iris_random_forest_model"
    model_promoter = ModelPromoter(model_name)

    version_to_promote = 1  # Example version
    model_promoter.promote_to_staging(version_to_promote)
