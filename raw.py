from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Oracle_Spark_Integration") \
    .config("spark.driver.extraClassPath", "/home/iauro/airflow/dags/app/jar_files/ojdbc8.jar").getOrCreate()

# JDBC properties
jdbc_url = "jdbc:oracle:thin:@//localhost:1521/FREEPDB1"
db_properties = {
    "user": "system",
    "password": "oracle",  # Use your actual password
    "driver": "oracle.jdbc.OracleDriver"
}

# Read table into Spark DataFrame
df = spark.read.jdbc(url=jdbc_url, table="HELP", properties=db_properties)

# Show the data
df.show()