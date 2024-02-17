import pandas as pd
import numpy as np

class DataFrameTransformer:
    def __init__(self):
        self.df = None

    def convert_data_types(self, data_df, column_types=None):
        if column_types:
            for column, data_type in column_types.items():
                data_df[column] = data_df[column].astype(data_type)

    def apply_feature_engineering(self, data_df, transformations=None):
        if transformations and data_df is not None:
            for new_column, expression in transformations.items():
                try:
                    data_df[new_column] = eval(expression, globals(), data_df)
                except Exception as e:
                    print(f"Error applying transformation for {new_column}: {e}")

    def remove_unnecessary_columns(self, data_df, columns_to_remove=None):
        if columns_to_remove:
            data_df = data_df.drop(columns=columns_to_remove)

    def handle_outliers(self, data_df, numerical_columns=None, lower_bound=None, upper_bound=None):
        if numerical_columns and lower_bound is not None and upper_bound is not None:
            for column in numerical_columns:
                q1 = data_df[column].quantile(0.25)
                q3 = data_df[column].quantile(0.75)
                iqr = q3 - q1
                lower_bound_val = q1 - 1.5 * iqr
                upper_bound_val = q3 + 1.5 * iqr
                data_df[column] = np.where((data_df[column] < lower_bound_val) | (data_df[column] > upper_bound_val),
                                           data_df[column].median(), data_df[column])


