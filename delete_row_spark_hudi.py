from hudi_config import get_hudi_configuration
from pyspark.sql import SparkSession

conf = get_hudi_configuration()

spark = SparkSession.builder.config(conf=conf).getOrCreate()

base_path = "file:///D:/Dev/PycharmProjects/hudiExperimental/spark-warehouse/trip_info"

df = spark.read.format("hudi").load(base_path).filter("uuid = '3f3d9565-7261-40e6-9b39-b8aa784f95e2'")

df.show(truncate=False)

hudi_hard_delete_options = {
    'hoodie.table.name': 'trip_info',
    'hoodie.datasource.write.partitionpath.field': 'city',
    'hoodie.datasource.write.operation': 'delete',
}

