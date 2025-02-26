from pyspark.sql.functions import col, collect_list
import json

def source_oracle_data(source_connector, source_name, database_type):
    
    query = """
        SELECT DISTINCT OWNER 
        FROM ALL_TABLES 
        WHERE OWNER = 'SYSTEM'
    """
    
    databases_df = source_connector.read_table(query)

    results = []
    default_tables = {"DUAL", "USER_TABLES", "ALL_TABLES", "DBA_TABLES"}

    for row in databases_df.collect():
        db_name = row["OWNER"]  

        table_query = f"SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = '{db_name}'"
        tables_df = source_connector.read_table(table_query)

        for table_row in tables_df.collect():
            table_name = table_row["TABLE_NAME"]

            # Skip default system tables
            if table_name in default_tables:
                continue

            count_query = f"SELECT COUNT(*) AS row_count FROM {db_name}.{table_name}"
            row_count_df = source_connector.read_table(count_query)

            row_count = int(row_count_df.collect()[0]["ROW_COUNT"])  

            schema_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM ALL_TAB_COLUMNS 
            WHERE OWNER = '{db_name}' 
            AND TABLE_NAME = '{table_name}'
            """
            schema_df = source_connector.read_table(schema_query)

            schema_dict = schema_df.agg(
                collect_list(col("COLUMN_NAME")).alias("columns"),
                collect_list(col("DATA_TYPE")).alias("datatypes")
            ).collect()[0]

            schema_json = json.dumps(dict(zip(schema_dict["columns"], schema_dict["datatypes"])))

            results.append((source_name, database_type, db_name, table_name, row_count, schema_json))

    schema = ["source_name", "database_type", "database_name", "table_name", "row_count", "table_schema"]
    oracle_df = source_connector.create_dataframe(results, schema)

    return oracle_df