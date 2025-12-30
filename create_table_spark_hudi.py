from hudi_config import get_hudi_configuration
from pyspark.sql import SparkSession

conf = get_hudi_configuration()

spark = SparkSession.builder.config(conf=conf).master("local[*]").appName("HudiTableCreation").getOrCreate()

print("Creating Hudi table 'trip_info'...")
spark.sql("""

CREATE TABLE IF NOT EXISTS trip_info (
    ts BIGINT,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING HUDI
LOCATION 'file:///D:/Dev/PycharmProjects/Hudi_Spark/spark-warehouse/trip_info'
PARTITIONED BY (city)
TBLPROPERTIES (
    type = 'cow',
    primaryKey = 'uuid',
    preCombineField = 'ts'
);
""")

print("Hudi table 'trip_info' created successfully.")

spark.stop()
