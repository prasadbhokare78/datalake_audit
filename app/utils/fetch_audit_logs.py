from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def fetch_audit_logs(connector, table_name):
    query = f"SELECT * FROM {table_name}"
    df = connector.read_table(query)

    window_spec = Window.partitionBy("source_name", "database_name", "table_name").orderBy(F.col("updated_at").desc())

    df = df.withColumn("row_number", F.row_number().over(window_spec))

    latest_df = df.filter(F.col("row_number") == 1).drop("row_number")

    filtered_df = latest_df.filter(F.col("flag") == True)

    records = filtered_df.select(
        "source_name", "database_type" ,"database_name", "table_name", "table_schema", "fetch_type", 
        "hour_interval", "mode", "batch_size", "executor_memory", "executor_cores", 
        "driver_memory", "min_executors", "max_executors", "initial_executors", "driver_cores", "date_col",
        "mod_date_column", "add_date_column", "min_date_column"
    ).collect()

    valid_records = [
        (
            row.source_name, row.database_type ,row.database_name, row.table_name, row.table_schema, row.fetch_type, 
            row.hour_interval, row.mode, row.batch_size, row.executor_memory, row.executor_cores, 
            row.driver_memory, row.min_executors, row.max_executors, row.initial_executors, row.driver_cores,
            row.date_col.strftime('%Y-%m-%d') if isinstance(row.date_col, datetime) else str(row.date_col), 
            row.mod_date_column, row.add_date_column, row.min_date_column
        )
        for row in records if None not in [
            row.source_name, row.database_type, row.database_name, row.table_name, row.table_schema, 
            row.fetch_type, row.mode, row.date_col, row.executor_memory, row.executor_cores, 
            row.driver_memory, row.min_executors, row.max_executors, row.initial_executors, row.driver_cores,
            row.mod_date_column, row.add_date_column, row.min_date_column
        ]
    ]

    return valid_records
