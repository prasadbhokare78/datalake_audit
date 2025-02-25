from app.connectors.oracle_connector import OracleConnector
from app.connectors.postgres_connector import PostgresConnector
from app.connectors.mssql_connector import MSSQLConnector
from app.utils.db_handler import DatalakeHandler

class DatalakeAuditHandler:
    def __init__(self, source_name, source_type, source_params, destination_params):
        self.source_name = source_name
        self.source_type = source_type
        self.source_params = source_params
        self.destination_params = destination_params

    def get_source_connector(self):
            """Returns the appropriate source database connector."""
            host=self.source_params.get("host")
            port=self.source_params.get("port")
            user=self.source_params.get("user")
            password=self.source_params.get("password")

            if self.source_type == "mssql":
                return MSSQLConnector(
                    host=host,
                    port=port,
                    user=user,
                    password=password
                )
            elif self.source_type == "postgresql":
                return PostgresConnector(
                    host=host,
                    port=port,
                    user=user,
                    password=password
                )
            elif self.source_type == "oracle":
                return OracleConnector(
                    host=host,
                    port=port,
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
    
    def get_database_names(self):
        """Get the names of the source and destination databases."""
        source_connector = self.get_source_connector()
        destination_connector = self.get_destination_connector()
        print('source_database', source_connector)
        print('destination_database', destination_connector)

        config_data = {
            "source_name": self.source_name,
            "source_type": self.source_type,
            "source_params": self.source_params,
            "destination_params": self.destination_params,
        }

        print(config_data)

        # print(self.destination_params)

        if self.source_type == "mssql":
            DatalakeHandler(config_data).mssql_handler(source_connector, destination_connector)
        elif self.source_type == "postgresql":
            DatalakeHandler(config_data).postgres_handler(source_connector, destination_connector)
        elif self.source_type == "oracle":
            DatalakeHandler(config_data).oracle_handler(source_connector, destination_connector)
        # else:
        #     raise ValueError("Unsupported database type")

        # print('done')
        
    def etl(self):

        try:
            self.get_database_names()
        except Exception as e:
            raise Exception(str(e))
        