# def source_oracle_data(source_connector, source_name, database_type):
#     # Step 1: Retrieve distinct database/schema names from Oracle
#     query = """
#         SELECT DISTINCT OWNER 
#         FROM ALL_TABLES 
#         WHERE OWNER = 'MYUSER' 
#     """
    
#     databases_df = source_connector.read_table(query)

#     results = []

#     # Step 2: Iterate over each schema and fetch table names
#     for row in databases_df.collect():
#         db_name = row["OWNER"]  

#         table_query = f"SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = '{db_name}'"
#         tables_df = source_connector.read_table(table_query)

#         for table_row in tables_df.collect():
#             table_name = table_row["TABLE_NAME"]

#             count_query = f"SELECT COUNT(*) AS row_count FROM {db_name}.{table_name}"
#             row_count_df = source_connector.read_table(count_query)

#             row_count = int(row_count_df.collect()[0]["ROW_COUNT"])  

#             # Fetch schema and convert to JSON format
#             schema_query = f"""
#             SELECT COLUMN_NAME, DATA_TYPE 
#             FROM ALL_TAB_COLUMNS 
#             WHERE OWNER = '{db_name}' 
#             AND TABLE_NAME = '{table_name}'
#             """
#             schema_df = source_connector.read_table(schema_query)

#             schema_dict = schema_df.agg(
#                 collect_list(col("COLUMN_NAME")).alias("columns"),
#                 collect_list(col("DATA_TYPE")).alias("datatypes")
#             ).collect()[0]

#             schema_json = json.dumps(dict(zip(schema_dict["columns"], schema_dict["datatypes"])))

#             results.append((source_name, database_type, db_name, table_name, row_count, schema_json))


#     # Step 3: Create a DataFrame from the results
#     schema = ["source_name", "database_type", "database_name", "table_name", "row_count", "table_schema"]
#     oracle_df = source_connector.create_dataframe(results, schema)

#     print("Oracle Data:")
#     oracle_df.show()

    # Step 4: Read existing PostgreSQL table
    # try:
    #     query = "(SELECT source_name, database_type, database_name, table_name, row_count FROM public.datalake_source_tracker) AS subquery"
    #     postgres_df = spark.read.jdbc(url=POSTGRES_JDBC_URL, table=query, properties=POSTGRES_PROPERTIES)
        
    #     # Add missing column `table_schema` with NULL values to match `oracle_df`
    #     postgres_df = postgres_df.withColumn("table_schema", col("row_count").cast("STRING"))
        
    #     print("PostgreSQL Data:")
    #     postgres_df.show()
    # except Exception as e:
    #     print(f"⚠️ Error reading from PostgreSQL: {e}")
    #     spark.stop()
    #     exit()

    # # Step 5: Compare Oracle and PostgreSQL Data
    # oracle_df = oracle_df.select(*schema)
    # postgres_df = postgres_df.select(*schema)

    # # Step 6: Handle Missing Tables
    # missing_in_postgres_df = oracle_df.join(
    #     postgres_df,
    #     on=["source_name", "database_type", "database_name", "table_name"],
    #     how="left_anti"
    # )

    # print("Missing in PostgreSQL:")
    # missing_in_postgres_df.show()

    # # Add timestamps
    # missing_in_postgres_df = missing_in_postgres_df.withColumn("created_at", current_date())
    # missing_in_postgres_df = missing_in_postgres_df.withColumn("updated_at", current_date().cast(DateType()))

    # # Ensure table_schema is of type STRING
    # missing_in_postgres_df = missing_in_postgres_df.withColumn("table_schema", col("table_schema").cast("STRING"))

    # return missing_in_postgres_df

# def source_mssql_data(source_connector, source_name, database_type):
#     exclude_dbs = ('master', 'tempdb', 'model', 'msdb')
    
#     databases_query = f"""
#         SELECT name 
#         FROM sys.databases 
#         WHERE name NOT IN {exclude_dbs}
#     """
#     databases_df = source_connector.read_table(databases_query)
    
#     results = []
    
#     for row in databases_df.collect():
#         db_name = row["name"]
#         print(f"Processing database: {db_name}")
#         source_connector.set_url(db_name)

#         table_query = """
#             SELECT TABLE_NAME 
#             FROM INFORMATION_SCHEMA.TABLES 
#             WHERE TABLE_TYPE = 'BASE TABLE'
#         """
        
#         try:
#             tables_df = source_connector.read_table(table_query)
            
#             for table_row in tables_df.collect():
#                 table_name = table_row["TABLE_NAME"]
                
#                 # Fetch row count
#                 count_query = f"SELECT COUNT(*) AS row_count FROM {table_name}"
#                 row_count_df = source_connector.read_table(count_query)
#                 row_count = row_count_df.collect()[0]["row_count"]
                
#                 # Fetch schema
#                 schema_query = f"""
#                     SELECT COLUMN_NAME, DATA_TYPE 
#                     FROM INFORMATION_SCHEMA.COLUMNS
#                     WHERE TABLE_NAME = '{table_name}'
#                 """
#                 schema_df = source_connector.read_table(schema_query)
                
#                 schema_dict = schema_df.select(
#                     collect_list(col("COLUMN_NAME")).alias("columns"),
#                     collect_list(col("DATA_TYPE")).alias("datatypes")
#                 ).collect()[0]
                
#                 schema_json = json.dumps(dict(zip(schema_dict["columns"], schema_dict["datatypes"])))
                
#                 results.append((source_name, database_type, db_name, table_name, row_count, schema_json))
                
#         except Exception as e:
#             print(f"Error processing {db_name}: {e}")
    
#     schema = ["source_name", "database_type", "database_name", "table_name", "row_count", "table_schema"]
#     source_mssql_df = source_connector.create_dataframe(results, schema)
    
#     print("MSSQL Source Data:")
#     source_mssql_df.show()

#     return source_mssql_df