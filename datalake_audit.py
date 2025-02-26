import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
from app.utils.audit_manager import DatalakeAuditHandler
from dotenv import load_dotenv # type: ignore 2
from pydantic import ValidationError # type: ignore

load_dotenv()

current_directory = os.path.dirname(os.path.abspath(__file__))
config_folder = os.path.join(current_directory, 'app', 'config')

def fetch_and_upload_data(source_name, source_type, source_params, destination_params):
    print(f"Processing source: {source_name}")
    print(f"Source Type: {source_type}")
    print(f"Source Parameters: {source_params}")
    print(f"Destination Parameters: {destination_params}")
    DatalakeAuditHandler(source_name, source_type, source_params, destination_params).etl()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 18),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

config_file_path = os.path.join(config_folder, 'test.json')
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
        print(config)
        for source in config['sources']:
            source_name = source['name']
            source_type = source['database_type']
            source_params = source['param']
            destination_params = config['destinations'][0]['param']
            task = PythonOperator(
                task_id=f"process_{source_name}",
                python_callable=fetch_and_upload_data,
                op_kwargs={
                    "source_name": source_name,
                    "source_type": source_type,
                    "source_params": source_params,
                    "destination_params": destination_params,
                },
                dag=dag,
            )


        globals()[dag_name] = dag
    except (FileNotFoundError, json.JSONDecodeError, KeyError, AttributeError) as e:
        print(f"Error processing config file: {e}")


