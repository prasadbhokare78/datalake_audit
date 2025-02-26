from pyspark.sql.functions import col, to_json, collect_list, struct, current_date
from pyspark.sql.types import DateType
import json


def update_postgres(missing_df, connector, table_name):
    try:
        if hasattr(missing_df, "count") and callable(getattr(missing_df, "count")):
            if missing_df.count() > 0:
                connector.write_table(missing_df, table_name)
                print("Successfully inserted missing entries into PostgreSQL.")
            else:
                print("No missing records to insert.")
        else:
            raise TypeError(f"Expected a PySpark DataFrame but got {type(missing_df)}")
    except Exception as e:
        print(f"Error inserting into PostgreSQL: {e}")


def missing_destination(destination_connector, source_df):
    schema = ["source_name", "database_type", "database_name", "table_name", "row_count", "table_schema"]
    print('breakpoint')
    
    try:
        query = "SELECT source_name, database_type, database_name, table_name, row_count, table_schema FROM public.datalake_source_tracker"
        dest_df = destination_connector.read_table(query)
        print("MSSQL Destination Data:")
        dest_df.show()
    except Exception as e:
        print(f"⚠️ Error reading from MSSQL Destination: {e}")
        destination_connector.stop_spark_session()
        exit()

    source_df = source_df.select(*schema)
    dest_df = dest_df.select(*schema)

    common_df = source_df.alias("source").join(
        dest_df.alias("dest"),
        on=["source_name", "database_type", "database_name", "table_name"],
        how="inner"
    )

    row_count_mismatch_df = common_df.filter(col("source.row_count") != col("dest.row_count"))

    missing_in_dest_df = source_df.join(
        dest_df,
        on=["source_name", "database_type", "database_name", "table_name"],
        how="left_anti"
    )

    print("Row Count Mismatches:")
    row_count_mismatch_df.show()

    print("Missing in PostgreSQL Destination:")
    missing_in_dest_df.show()

    missing_in_dest_df = missing_in_dest_df.withColumn("created_at", current_date())
    missing_in_dest_df = missing_in_dest_df.withColumn("updated_at", current_date().cast(DateType()))

    missing_in_dest_df = missing_in_dest_df.withColumn("table_schema", col("table_schema").cast("STRING"))

    return missing_in_dest_df
