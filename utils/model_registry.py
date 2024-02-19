import mlflow

class ModelRegistry:
    def __init__(self):
        pass

    def register_model(self, model_identifier, model_name):
        try:
            if "runs:/" in model_identifier:
                mlflow.register_model(model_identifier, model_name)
                print(f"Model registered: {model_name}")
            else:
                # Assuming model_identifier is a run_id
                model_uri = f"runs:/{model_identifier}/random_forest_model"
                mlflow.register_model(model_uri, model_name)
                print(f"Model registered: {model_name}")
        except Exception as e:
            print(f"Error registering model {model_name}: {e}")


# # Example usage:
# if __name__ == "__main__":
#     model_registry = ModelRegistry()

#     # Register a model using run_id
#     run_id_or_model_uri = "4c22f6af34cb458ebe5ad5b68dab806f"
#     registered_model_name = "Used_Car_price_prediction"
#     model_registry.register_model(run_id_or_model_uri, registered_model_name)
