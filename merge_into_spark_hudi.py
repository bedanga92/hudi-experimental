from hudi_config import get_hudi_configuration
from pyspark.sql import SparkSession

conf = get_hudi_configuration()

spark = SparkSession.builder.config(conf=conf).master("local[*]").appName("HudiTableCreation").getOrCreate()

spark.sql("""
CREATE TABLE fare_adjustment (
  ts BIGINT,
  uuid STRING,
  rider STRING,
  driver STRING,
  fare DOUBLE,
  city STRING
) USING HUDI
LOCATION 'file:///D:/Dev/PycharmProjects/hudiExperimental/spark-warehouse/fare_adjustment'
""")

spark.sql("""
INSERT INTO fare_adjustment VALUES
  (1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',-2.70 ,'san_francisco'),
  (1695530237068,'3f3d9565-7261-40e6-9b39-b8aa784f95e2','rider-K','driver-U',64.20 ,'san_francisco'),
  (1695241330902,'ea4c36ff-2069-4148-9927-ef8c1a5abd24','rider-H','driver-R',66.60 ,'sao_paulo'),
  (1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',1.85,'chennai')
""")


spark.sql("""
CREATE TABLE trip_info
USING HUDI
LOCATION 'file:///D:/Dev/PycharmProjects/hudiExperimental/spark-warehouse/trip_info'
""")

# Merge into target Hudi table
spark.sql("""
MERGE INTO trip_info AS target
USING fare_adjustment AS source
ON target.uuid = source.uuid

WHEN MATCHED THEN UPDATE SET
  target.fare = target.fare + source.fare,
  target.ts = source.ts

WHEN NOT MATCHED THEN INSERT (
  ts,
  uuid,
  rider,
  driver,
  fare,
  city
) VALUES (
  source.ts,
  source.uuid,
  source.rider,
  source.driver,
  source.fare,
  source.city
)

""")

print("Merge operation completed successfully.")