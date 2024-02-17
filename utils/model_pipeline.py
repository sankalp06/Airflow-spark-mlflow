from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import pandas as pd


class ModelPipeline:
    def __init__(self, target_column):
        self.target_column = target_column
        self.preprocessor = None
        self.pipeline = None

    def separate_features_target(self, df):
        X = df.drop(self.target_column, axis=1)
        y = df[self.target_column]
        return X, y

    def identify_features(self, X):
        numerical_features = X.select_dtypes(include=['int64', 'float64']).columns
        categorical_features = X.select_dtypes(include=['object']).columns
        return numerical_features, categorical_features

    def create_preprocessor(self, numerical_features, categorical_features):
        numerical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', StandardScaler())
        ])

        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('onehot', OneHotEncoder(handle_unknown='ignore'))
        ])

        self.preprocessor = ColumnTransformer(
            transformers=[
                ('num', numerical_transformer, numerical_features),
                ('cat', categorical_transformer, categorical_features)
            ])
        
    def create_pipeline(self, classifier):
        self.pipeline = Pipeline(steps=[
            ('preprocessor', self.preprocessor),
            ('classifier', classifier)
        ])

    def train_pipeline(self, X_train, y_train):
        self.pipeline.fit(X_train, y_train)
        return self.pipeline

    def evaluate_pipeline(self, X_test, y_test):
        accuracy = self.pipeline.score(X_test, y_test)
        print(f'Model Accuracy: {accuracy}')
        return accuracy
