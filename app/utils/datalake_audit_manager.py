from app.connectors.oracle_connector import OracleConnector
from app.connectors.postgres_connector import PostgresConnector
from app.connectors.mssql_connector import MSSQLConnector
from app.utils.datalake_audit.audit import source_mssql_data, source_oracle_data, source_postgres_data, update_postgres, missing_destination

class DatalakeAuditHandler:
    def __init__(self, source_name, source_type, source_params, destination_params):
        self.source_name = source_name
        self.source_type = source_type
        self.source_params = source_params
        self.destination_params = destination_params

    def get_source_connector(self):
        """Returns the appropriate source database connector."""
        # connector_map = {
        #     "mssql": MSSQLConnector,
        #     "oracle": OracleConnector,
        #     "postgresql": PostgresConnector
        # }
        
        # connector_class = connector_map.get(self.source_type.lower())
        # if not connector_class:
        #     raise ValueError(f"Unsupported database type: {self.source_type}")

        # Unpack parameters and pass them one by one

        # return connector_class(
        #     host=self.source_params.get("host"),
        #     port=self.source_params.get("port"),
        #     user=self.source_params.get("user"),
        #     password=self.source_params.get("password")
        # )

        host=self.destination_params.get("host")
        port=self.destination_params.get("port")
        database=self.destination_params.get("database")
        user=self.destination_params.get("user")
        password=self.destination_params.get("password")
        

        if self.source_type == "mssql":
            return MSSQLConnector(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
        elif self.source_type == "postgresql":
            return PostgresConnector(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
        elif self.source_type == "oracle":
            return OracleConnector(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )


    def get_destination_connector(self):
        """Returns the PostgreSQL destination connector."""
        return PostgresConnector(
            host=self.destination_params.get("host"),
            port=self.destination_params.get("port"),
            database=self.destination_params.get("database"),
            user=self.destination_params.get("user"),
            password=self.destination_params.get("password")
        )
    
    def get_database_names(self, source_connector, df):
        
        if self.source_type == "postgresql":
            # missing_df = source_postgres_data(source_connector, self.source_name, self.source_type)
            # missing_df.show()
            # destination_connector = self.get_destination_connector()
            # print("brooooo",destination_connector)
            # df = destination_connector.read_table('SELECT source_name, database_type, database_name, table_name, row_count, table_schema FROM public.datalake_source_tracker')
            # print(df)
            # # update_postgres(destination_connector, missing_df, table_name=self.destination_params.get('table_name'))
            pass
        if self.source_type == "mssql":
            missing_df = source_mssql_data(source_connector, self.source_name, self.source_type)
            missing_df.show()

            # update_postgres(destination_connector, missing_df, table_name=self.destination_params.get('table_name'))
            
        # elif self.source_type == "oracle":
        #     query = "SELECT name FROM v$database"
        #     df = source_oracle_data(source_connector, destination_connector)
        #     update_postgres(source_connector, destination_connector, df)
        else:
            raise ValueError("Unsupported database type")

        # Fetch database names
        # df = connector.read_table(query)
        # df.show()

    def etl(self):
        print('hello world')
        destination_connector = self.get_destination_connector()
        print("brooooo",destination_connector)
        df = destination_connector.read_table('SELECT source_name, database_type, database_name, table_name, row_count, table_schema FROM datalake_source_tracker')
        df.show()
        source_connector = self.get_source_connector()
        # destination_connector = self.get_destination_connector()

        print("Source Connector: ",source_connector)

        dfd = source_connector.read_table("""SELECT name 
                FROM sys.databases 
                WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')""")
        dfd.show()
        # print('Destination Connector: ', destination_connector)

        # print(self.destination_params)

        # df = destination_connector.read_table('SELECT source_name, database_type, database_name, table_name, row_count, table_schema FROM public.datalake_source_tracker')
        # df.show()

        try:
            self.get_database_names(source_connector, df)
            # df = source_connector.read_table('SELECT datname FROM pg_database WHERE datistemplate = false')
            # print(df)
        except Exception as e:
            raise Exception(str(e))
        # query = "(SELECT name FROM sys.databases) AS db_list"

        # df = source_connector.read_table(query)

        # print("Destination Connector: ", df)