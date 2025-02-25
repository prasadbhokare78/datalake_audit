from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
import os
import json

# Define the Python function
def print_hello():
    print("Hello, Airflow!")
    current_directory = os.path.dirname(os.path.abspath(__file__))
    config_folder = os.path.join(current_directory, 'app', 'config')
    print(config_folder)
    config_file_path = os.path.join(config_folder, 'config_update.json')
    if os.path.exists(config_file_path):
        try:
            with open(config_file_path) as f:
                config = json.load(f)
            
            dag_name = config['dag_config']['dag_name']
            # schedule_time = config['dag_config']['schedule_time']
            print(config)
            print(dag_name,)
        except (FileNotFoundError, json.JSONDecodeError, KeyError, AttributeError) as e:
            print(f"Error processing config file: {e}") 

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "simple_hello_dag",  # DAG ID
    default_args=default_args,
    description="A simple DAG that prints Hello, Airflow!",
    schedule_interval="@daily",  # Runs daily
    catchup=False,
) as dag:

    # Define the task
    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello,
    )

    # Define task dependencies (optional here)
    hello_task  # If multiple tasks exist, link them using `>>`

