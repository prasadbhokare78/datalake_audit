import datetime
from datetime import datetime, timedelta

def fetch_audit_logs(connector):

    query = "select * from datalake_source_tracker"
    df = connector.read_table(query)
    
    last_24_hours = datetime.now() - timedelta(days=1)
    
    filtered_df = df.filter((df["updated_at"] >= last_24_hours) & (df["flag"] == True))

    records = filtered_df.select("source_name", "database_name", "table_name", "table_schema", "fetch_type", 
                                 "hour_interval", "mode", "batch_size", "date_col", "executor_memory", "executor_cores", 
                                 "driver_memory", "min_executors", "max_executors", "initial_executors", "driver_cores").collect()
    
    valid_records = [
        (
            row.source_name, row.database_name, row.table_name, row.table_schema, row.fetch_type, 
            row.hour_interval, row.mode, row.batch_size, row.executor_memory, row.executor_cores, 
            row.driver_memory, row.min_executors, row.max_executors, row.initial_executors, row.driver_cores,
            row.date_col.strftime('%Y-%m-%d') if isinstance(row.date_col, datetime) else str(row.date_col)
        )
        for row in records if None not in [
            row.source_name, row.database_name, row.table_name, row.table_schema, 
            row.fetch_type, row.mode, row.date_col, row.executor_memory, row.executor_cores, 
            row.driver_memory, row.min_executors, row.max_executors, row.initial_executors, row.driver_cores
        ]
    ]

        
    return valid_records
