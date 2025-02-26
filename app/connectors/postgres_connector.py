from pyspark.sql import SparkSession
import time
from app.jar_files.jar_manager import JarManager
from pyspark.sql.functions import year as spark_year, month as spark_month, dayofmonth as spark_dayofmonth, col


class PostgresConnector:
    def __init__(self, host, port, user, password, database=""):
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        self.driver = "org.postgresql.Driver"
        self.user = user
        self.password = password
        self.spark_attempts = 0
        self.read_attempts = 0
        self.write_attempts = 0
        self.max_retries = 3
        self.retry_delay = 5
        self.host = host
        self.port = port
        
        jar_manager = JarManager(
            required_jars=[
                'postgresql-42.7.4.jar',
                'ojdbc8.jar',
                'mssql-jdbc-12.8.1.jre11.jar'
            ]
        )

        self.jdbc_drivers_path = jar_manager.get_jars()
        self.all_jdbc_drivers_path = ",".join(self.jdbc_drivers_path)

        while self.spark_attempts < self.max_retries:
            try:
                self.spark = SparkSession.builder \
                    .appName("PostgresConnector") \
                    .config("spark.jars", self.all_jdbc_drivers_path) \
                    .getOrCreate()
                break
            except Exception as e:
                self.spark_attempts += 1
                print(f"Failed to create Spark session. Attempt {self.spark_attempts} of {self.max_retries}. Error: {e}")
                if self.spark_attempts < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    raise Exception(str(e))

                
    def set_url(self, database):
        """Set the schema (namespace) for PostgreSQL queries."""
        self.jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{database}"
        

    def read_table(self, query):
        while self.read_attempts < self.max_retries:
            try:
                return self.spark.read \
                    .format("jdbc") \
                    .option("url", self.jdbc_url) \
                    .option("query", query) \
                    .option("user", self.user) \
                    .option("password", self.password) \
                    .option("driver", self.driver) \
                    .load()
            except Exception as e:
                self.read_attempts += 1
                print(f"Failed to read data. Attempt {self.read_attempts} of {self.max_retries}. Error: {e}")
                if self.read_attempts < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    raise Exception(str(e))
                
    def create_dataframe(self, results, schema_list):
        """
        Create a Spark DataFrame from a list of tuples and a schema list.
        
        :param results: List of tuples containing the data.
        :param schema_list: List of column names as strings.
        :return: Spark DataFrame.
        """
        try:
            df = self.spark.createDataFrame(results, schema=schema_list)
            return df
        except Exception as e:
            print(f"Error creating DataFrame: {e}")
            raise Exception(str(e))

    def write_table(self, df, table_name, mode="append"):
        """
        Write a Spark DataFrame to a PostgreSQL table.
        
        :param df: Spark DataFrame to write.
        :param table_name: Name of the table to write into.
        :param mode: Writing mode ('append', 'overwrite', etc.). Default is 'append'.
        """

        while self.write_attempts < self.max_retries:
            try:
                df.write \
                    .format("jdbc") \
                    .option("url", self.jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", self.user) \
                    .option("password", self.password) \
                    .option("driver", self.driver) \
                    .mode(mode) \
                    .save()
                print(f"Data successfully written to {table_name}.")
                return
            except Exception as e:
                self.write_attempts += 1
                print(f"Failed to write data. Attempt {self.write_attempts} of {self.max_retries}. Error: {e}")
                if self.write_attempts < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    raise Exception(str(e))
                
    def write_to_s3(self, df_data, destination_path, date_column):
        try:
            df_data = df_data.withColumn("year", spark_year(col(date_column))) \
                             .withColumn("month", spark_month(col(date_column))) \
                             .withColumn("day", spark_dayofmonth(col(date_column)))
            print(f"DataFrame columns after adding partition columns: {df_data.columns}")

            df_data.write.partitionBy("year", "month", "day") \
                         .mode("append") \
                         .parquet(destination_path)
            print(f"Uploaded data to S3 path: {destination_path}")
        except Exception as e:
            raise Exception(str(e))

    def stop_spark_session(self):
        try:
            self.spark.stop()
            print("Spark session stopped successfully.")
        except Exception as e:
            raise Exception(str(e))