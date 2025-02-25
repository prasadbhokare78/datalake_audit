from pyspark.sql.functions import col, to_json, collect_list, struct, current_date
import json

def source_postgres_data(source_connector, source_name, database_type):
    exclude_dbs = ('datalake_audit', 'postgres', 'template0', 'template1')
    
    query = f"SELECT datname FROM pg_database WHERE datname NOT IN {exclude_dbs}"
    databases_df = source_connector.read_table(query)

    results = []
    for row in databases_df.collect():
        db_name = row["datname"]
        if db_name == 'datalake_audit': 
            continue
        
        print(f"Processing database: {db_name}")
        source_connector.set_url(db_name)

        table_query = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """

        try:
            tables_df = source_connector.read_table(table_query)
            tables_df.show()

            for table_row in tables_df.collect():
                table_name = table_row["table_name"]

                count_query = f"SELECT COUNT(*) AS row_count FROM public.{table_name}"
                row_count_df = source_connector.read_table(count_query)
                row_count = row_count_df.collect()[0]["row_count"]

                schema_query = f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                """
                schema_df = source_connector.read_table(schema_query)

                print(f"Table: {table_name}, Row Count: {row_count}")
                schema_df.show()

            schema_dict = schema_df.select(
                collect_list(col("column_name")).alias("columns"),
                collect_list(col("data_type")).alias("datatypes")
            ).collect()[0]

            schema_json = json.dumps(dict(zip(schema_dict["columns"], schema_dict["datatypes"])))

            results.append((source_name, database_type, db_name, table_name, row_count, schema_json))
            
        except Exception as e:
            print(str(e))
        
    schema = ["source_name", "database_type", "database_name", "table_name", "row_count", "table_schema"]
    source_postgres_df = source_connector.create_dataframe(results, schema)

    print("PostgreSQL Source Data:")
    source_postgres_df.show()

    return source_postgres_df
 