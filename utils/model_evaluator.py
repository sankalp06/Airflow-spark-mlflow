
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

class ModelEvaluator:
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def evaluate_classification_model(self, X_test, y_test):
        y_pred = self.pipeline.predict(X_test)
        accuracy = self.pipeline.score(X_test, y_test)
        mae = mean_absolute_error(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)
        r_squared = r2_score(y_test, y_pred)

        return {
        "MSE": accuracy,
        "RMSE": mse,
        "MAE": mae,
        "R-squared": r_squared
        }

        
    def evaluate_regression_model(self, X_test, y_test):
        y_pred = self.pipeline.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)
        r_squared = r2_score(y_test, y_pred)
        return {
            "MAE":mae, 
            "MSE": mse, 
            "R2": r_squared
            }
