from pyspark.sql import SparkSession
# from app.utils.s3_utils import S3Utils
import time
from app.jar_files.jar_manager import JarManager
from pyspark.sql.functions import year as spark_year, month as spark_month, dayofmonth as spark_dayofmonth, col
# from app.constants.constant_error import SparkSessionError, DataReadError, DataWriteError

class OracleConnector:
    def __init__(self, host, port, user, password, service="FREEPDB1"):
        self.jdbc_url = f"jdbc:oracle:thin:@{host}:{port}/{service}"
        self.jar_path = "/home/iauro/airflow/dags/app/jar_files/ojdbc8.jar"
        self.driver = "oracle.jdbc.OracleDriver"
        self.user = user
        self.password = password
        self.spark_attempts = 0
        self.read_attempts = 0
        self.max_retries = 3
        self.retry_delay = 5
        
        jar_manager = JarManager(
            required_jars=[
                'ojdbc8.jar'
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

            # Write the DataFrame to S3 with partitioning
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