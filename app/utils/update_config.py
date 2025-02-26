import os
import json
import time
from app.utils.fetch_audit_logs import fetch_audit_logs
from app.connectors.postgres_connector import PostgresConnector

class UpdateConfig():
    def __init__(self, source_name, source_type, source_params, schedule_time):
        self.source_name = source_name
        self.source_type = source_type
        self.source_params = source_params
        self.schedule_time = schedule_time
        
        self.script_dir = os.path.abspath(__file__)
        self.app_dir = os.path.dirname(os.path.dirname(self.script_dir))
        self.config_path = os.path.join(self.app_dir, "config", "config.json")

        if os.path.exists(self.config_path):
            with open(self.config_path, "r") as f:
                self.config_file = json.load(f)

    def get_connector(self):
        """Returns the PostgreSQL destination connector."""
        return PostgresConnector(
            host=self.source_params.get("host"),
            port=self.source_params.get("port"),
            user=self.source_params.get("user"),
            password=self.source_params.get("password"),
            database=self.source_params.get("database"),
        )

    def update_config_files(self):
        script_dir = os.path.abspath(__file__)
        app_dir = os.path.dirname(os.path.dirname(script_dir))
        config_dir = os.path.join(app_dir, "config", "db_config")
        os.makedirs(config_dir, exist_ok=True) 

        connector = self.get_connector()
        
        audit_logs = fetch_audit_logs(connector)
        print(audit_logs)
        if not audit_logs:
            print("No audit logs found. Exiting.")
            return

        db_configs = {} 
        
        for log in audit_logs:
            source_name, database_name, table_name, table_schema, fetch_type, hour_interval, mode, batch_size, executor_memory, executor_cores, driver_memory, min_executors, max_executors, initial_executors, driver_cores, date_col = log

            try:
                table_schema = json.loads(table_schema)  
            except json.JSONDecodeError:
                print(f"Failed to parse JSON for {table_name}. Using empty schema.")
                table_schema = {}

            key = (source_name, database_name)
            if key not in db_configs:
                db_configs[key] = {
                    "dag_config": {
                        "dag_name": f"{source_name}_{database_name}_config_dag".lower(),
                        "schedule_time": self.schedule_time
                    },
                    "sources": {},
                    "destination": {
                        "name": self.source_name,
                        "params": self.source_params
                    },
                    "table_queries": []
                }

                matching_sources = [s for s in self.config_file["sources"] if s["name"] == source_name]
                if matching_sources:
                    db_configs[key]["sources"] = matching_sources[0]
                else:
                    print(f"No matching source found for {source_name}. Skipping.")
                    continue

            total_count_query = f"SELECT count(*) AS total_count FROM {table_name} WHERE {date_col} >= '{{start_date_time}}' AND {date_col} < '{{end_date_time}}'"
            if fetch_type == "batch":
                total_count_query += " ORDER BY {date_col} ASC, name ASC OFFSET {offset} LIMIT {batch_size}"

            table_params = {
                "data_query": f"SELECT * FROM {table_name} WHERE {date_col} >= '{{start_date_time}}' AND {date_col} < '{{end_date_time}}'",
                "date_column": date_col,
                "min_date_query": f"SELECT min({date_col}) AS min_date FROM {table_name}",
                "total_count_query": total_count_query,
                "schema": table_schema,
                "fetch_type": fetch_type,
                "mode": mode,
                "executor_memory": executor_memory,
                "executor_cores": executor_cores,
                "driver_memory": driver_memory,
                "min_executors": min_executors,
                "max_executors": max_executors,
                "initial_executors": initial_executors,
                "driver_cores": driver_cores
            }

            if fetch_type == "batch":
                table_params["batch_size"] = batch_size
                table_params["batch_interval_in_hr"] = hour_interval

            db_configs[key]["table_queries"].append({
                "table_name": table_name,
                "table_params": table_params
            })

        for (source_name, database_name), config_data in db_configs.items():
            file_name = f"{source_name}_{database_name}.json".lower()
            file_path = os.path.join(config_dir, file_name)

            print(f"Writing config to {file_path}")
            with open(file_path, "w") as f:
                json.dump(config_data, f, indent=4, default=str)
