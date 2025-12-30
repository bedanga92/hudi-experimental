from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os
import sys

# Ensure PySpark uses the correct Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# --- CONFIGURATION ---
AWS_REGION = "us-east-1"  # Set to your specific region
DATABASE_NAME = "hudi_experimental_db"
TABLE_NAME = "faker_hudi"
S3_PATH = f"s3a://spark-hudi-experimental/target/tbl/{TABLE_NAME}/"

# 1. Initialize SparkSession with the Glue Metastore Bridge
spark = SparkSession.builder \
    .appName("Local Spark HMS Bridge to Glue") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.jars.packages",
            "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("spark.hadoop.aws.region", AWS_REGION) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# 2. Create Sample Data
data = [
    (1, "John Doe", "john@example.com", "2024-01-15"),
    (2, "Jane Smith", "jane@example.com", "2024-01-16")
]
df = spark.createDataFrame(data, ["id", "name", "email", "date"]).withColumn("updated_at", current_timestamp())

# 3. Define Hudi Options
hudi_options = {
    'hoodie.table.name': TABLE_NAME,
    'hoodie.datasource.write.recordkey.field': 'id',
    'hoodie.datasource.write.partitionpath.field': 'date',
    'hoodie.datasource.write.precombine.field': 'updated_at',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.hive_style_partitioning': 'true',

    # Sync Logic: Use 'hms' because the Spark Session is now the HMS Client for Glue
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.mode': 'hms',
    'hoodie.datasource.hive_sync.database': DATABASE_NAME,
    'hoodie.datasource.hive_sync.table': TABLE_NAME,
    'hoodie.datasource.hive_sync.partition_fields': 'date',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
    'hoodie.datasource.hive_sync.use_jdbc': 'false',

    # Local parallelism tuning
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

# 4. Write to S3 and Sync to Glue
print(f"Writing data to S3 and syncing via HMS Bridge to Glue...")
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(S3_PATH)

# 5. Query back via the Catalog to prove the bridge is working
print(f"Querying Glue Catalog table: {DATABASE_NAME}.{TABLE_NAME}")
try:
    spark.sql(f"SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}").show()
except Exception as e:
    print(f"Query failed: {e}")

spark.stop()
