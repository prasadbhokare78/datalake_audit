from pyspark.sql import SparkSession
import time
from app.jar_files.jar_manager import JarManager
from pyspark.sql.functions import year as spark_year, month as spark_month, dayofmonth as spark_dayofmonth, col

class OracleConnector:
    def __init__(self, host, port, user, password):
        self.service = "FREEPDB1"
        self.jdbc_url = f"jdbc:oracle:thin:@{host}:{port}/{self.service}"
        self.driver = "oracle.jdbc.OracleDriver"
        self.user = user
        self.password = password
        self.spark_attempts = 0
        self.read_attempts = 0
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
                    .appName("OracleDBConnection") \
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
        self.jdbc_url = f"jdbc:oracle:thin:@{self.host}:{self.port}/{self.service}"

    def reinit_spark_session(self):
        try:
            self.spark.stop()
            self.spark = SparkSession.builder \
                .appName("PostgresConnector") \
                .config("spark.jars", self.jar_path) \
                .getOrCreate()
            print("Spark session reinitialized successfully.")
        except Exception as e:
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
            return []
        
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