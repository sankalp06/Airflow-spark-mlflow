from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn

# Define default arguments
default_args = {
    'owner': 'sankalp',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define the DAG
dag = DAG(
    'ml_model_training',
    default_args=default_args,
    description='A DAG to train and log a machine learning model using MLflow',
    schedule_interval='@daily'
)

# Define the training function
def train_ml_model():
    # Load the iris dataset
    iris = load_iris()
    X = iris.data
    y = iris.target

    # Split the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize the random forest classifier
    rf_clf = RandomForestClassifier(n_estimators=100, random_state=42)

    # Train the classifier
    rf_clf.fit(X_train, y_train)

    # Make predictions on the test set
    y_pred = rf_clf.predict(X_test)

    # Calculate accuracy
    accuracy = accuracy_score(y_test, y_pred)
    print("Accuracy:", accuracy)
    mlflow_tracking_uri = "http://host.docker.internal:5000"
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment("experiment_name")

    # Log the model and its parameters using MLflow
    with mlflow.start_run():
        # Log the parameters of the model
        mlflow.log_param("n_estimators", 100)
        mlflow.log_param("random_state", 42)

        # Log the metrics
        mlflow.log_metric("accuracy", accuracy)

        # Log the trained model
        mlflow.sklearn.log_model(rf_clf, "random_forest_model")

def predictt():
    import mlflow
    mlflow_tracking_uri = "http://host.docker.internal:5000"
    mlflow.set_tracking_uri(mlflow_tracking_uri)

    logged_model = 'runs:/a0a79812eae3475895c609ee260bd1c8/random_forest_model'

    # Load model as a PyFuncModel.
    loaded_model = mlflow.pyfunc.load_model(logged_model)
    iris = load_iris()
    X = iris.data
    y = iris.target

    # Split the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print(loaded_model.predict(X_test))
    
# Define the PythonOperator to train the ML model
train_ml_model_task = PythonOperator(
    task_id='train_ml_model',
    python_callable=train_ml_model,
    dag=dag
)

pre = PythonOperator(
    task_id='predict',
    python_callable=predictt,
    dag=dag
)

# Set task dependencies
train_ml_model_task >> pre
