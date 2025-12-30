from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from hudi_config import get_hudi_configuration

conf = get_hudi_configuration()
spark = SparkSession.builder.config(conf=conf).getOrCreate()

base_path = "file:///D:/Dev/PycharmProjects/hudiExperimental/spark-warehouse/trip_info"

# Read table
df = spark.read.format("hudi").load(base_path)

# Prepare updates
updatesDf = df.withColumn(
    "fare",
    when(col("rider") == "rider-D", 45.0).otherwise(col("fare"))
)

# Write updates (upsert)
hudi_options = {
    'hoodie.table.name': 'trip_info',
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.datasource.write.partitionpath.field': 'city',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.metadata.enable': 'false'
}
print("I am here 1")

updatesDf.write.format("hudi").options(**hudi_options).mode("append").save(base_path)
print("I am here 2")
# Read back and register temp view for SQL queries
spark.read.format("hudi").option("hoodie.metadata.enable", "false").load(base_path) \
    .createOrReplaceTempView("trip_info")

print("I am here 3")
spark.sql("SELECT * FROM trip_info WHERE rider = 'rider-D'").show()
