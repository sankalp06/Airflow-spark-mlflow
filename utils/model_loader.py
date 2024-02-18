import mlflow
from pyspark.sql.functions import struct, col

class MLflowModelLoader:
    def __init__(self, model_uri):
        self.model_uri = model_uri
        self.loaded_model = mlflow.pyfunc.load_model(self.model_uri)
    
    def predict_spark_dataframe(self, spark, df):
        loaded_model_udf = mlflow.pyfunc.spark_udf(spark, model_uri=self.model_uri)
        return df.withColumn('predictions', loaded_model_udf(struct(*map(col, df.columns))))
    
    def predict_pandas_dataframe(self, pandas_df):
        return self.loaded_model.predict(pandas_df)

