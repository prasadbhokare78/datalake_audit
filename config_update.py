import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
from app.utils.update_config import UpdateConfig
from dotenv import load_dotenv  # type: ignore

load_dotenv()

current_directory = os.path.dirname(os.path.abspath(__file__))
config_folder = os.path.join(current_directory, 'app', 'config')

def update_config(source_name, source_type, source_params, schedule_time):
    UpdateConfig(source_name, source_type, source_params, schedule_time).update_config_files()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

config_file_path = os.path.join(config_folder, 'config_update.json')
if os.path.exists(config_file_path):
    try:
        with open(config_file_path) as f:
            config = json.load(f)
        
        dag_name = config['dag_config']['dag_name']
        schedule_time = config['dag_config']['schedule_time']
        
        dag = DAG(
            dag_id=dag_name,
            default_args=default_args,
            description=f"Dynamic DAG for {dag_name}",
            schedule_interval=schedule_time,
            catchup=False,
        )

        source_name = config['source'][0]['name']
        source_type = config['source'][0]['database_type']
        source_params = config['source'][0]['param']
        schedule_time = config['dag_config']['schedule_time']
        task = PythonOperator(
            task_id=dag_name,
            python_callable=update_config,
            op_kwargs={
                "source_name": source_name,
                "source_type": source_type,
                "source_params": source_params,
                "schedule_time": schedule_time  
                },
            dag=dag,
        )
        globals()[dag_name] = dag
    except (FileNotFoundError, json.JSONDecodeError, KeyError, AttributeError) as e:
        print(f"Error processing config file: {e}") 