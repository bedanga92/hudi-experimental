from hudi_config import get_hudi_configuration
from pyspark.sql import SparkSession


conf = get_hudi_configuration()

spark = SparkSession.builder.config(conf=conf).getOrCreate()

print("Loading existing HUDI table trip_info...")

sc = spark.sparkContext
print("fs.defaultFS =", sc._jsc.hadoopConfiguration().get("fs.defaultFS"))

base_path = "file:///D:/Dev/PycharmProjects/hudiExperimental/spark-warehouse/trip_info"

df = (
    spark.read
    .format("hudi")
    .option("hoodie.metadata.enable", "false")
    .load(base_path)
)

df.show(100, truncate=False)

print("Displaying data from trip_info table:")
#
# spark.sql("SELECT * FROM trip_info").show()

