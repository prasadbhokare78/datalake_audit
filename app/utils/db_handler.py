from app.utils.datalake_audit.audit import source_oracle_data, update_postgres, missing_destination
from app.utils.datalake_audit.source_postgres import source_postgres_data
from app.utils.datalake_audit.source_mssql import source_mssql_data
from app.utils.datalake_audit.source_oracle import source_oracle_data

class DatalakeHandler:
    def __init__(self, config_data):
        self.source_name = config_data.get("source_name")
        self.source_type = config_data.get("source_type")
        self.source_params = config_data.get("source_params")
        self.destination_params = config_data.get("destination_params")
        self.destination_table = config_data.get("destination_params", {}).get("table_name")
        

    def postgres_handler(self, source_connector, destination_connector):
        """Returns the PostgreSQL destination connector."""

        fetch_data = source_postgres_data(source_connector, self.source_name, self.source_type)
        missing_data = missing_destination(destination_connector, fetch_data)
        update_postgres(missing_data, destination_connector, self.destination_table)

    def mssql_handler(self, source_connector, destination_connector):
        """Returns the MSSQL destination connector."""
        
        fetch_data = source_mssql_data(source_connector, self.source_name, self.source_type)
        missing_data = missing_destination(destination_connector, fetch_data)
        update_postgres(missing_data, destination_connector, self.destination_table)

    def oracle_handler(self, source_connector, destination_connector):
        """Returns the Oracle destination connector."""
        
        fetch_data = source_oracle_data(source_connector, self.source_name, self.source_type)
        missing_data = missing_destination(destination_connector, fetch_data)
        update_postgres(missing_data, destination_connector, self.destination_table)