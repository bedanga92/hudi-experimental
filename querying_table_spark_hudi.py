from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from hudi_config import get_hudi_configuration

# ------------------------------------------------------------------
# Spark session
# ------------------------------------------------------------------
conf = get_hudi_configuration()

spark = (
    SparkSession.builder
    .config(conf=conf)
    .getOrCreate()
)

# ------------------------------------------------------------------
# Verify filesystem
# ------------------------------------------------------------------
sc = spark.sparkContext
print("fs.defaultFS =", sc._jsc.hadoopConfiguration().get("fs.defaultFS"))

# ------------------------------------------------------------------
# Read Hudi table
# ------------------------------------------------------------------
base_path = "file:///D:/Dev/PycharmProjects/hudiExperimental/spark-warehouse/trip_info"

df = (
    spark.read
    .format("hudi")
    .option("hoodie.metadata.enable", "false")
    .load(base_path)
)

df.show()

# ------------------------------------------------------------------
# Query
# ------------------------------------------------------------------
df.where(col("fare") > 20).show()

df.createOrReplaceTempView("trip_info")

spark.sql("""
    SELECT uuid, fare, city
    FROM trip_info
    WHERE fare > 20
""").show()


