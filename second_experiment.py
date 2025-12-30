from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import os
import sys

# Ensure Spark uses the correct Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Constants - Update with your specific AWS details
AWS_REGION = "us-east-1"
DATABASE_NAME = "hudi_experimental_db"
TABLE_NAME = "faker_hudi"
S3_PATH = f"s3a://spark-hudi-experimental/target/tbl/{TABLE_NAME}/"

spark = SparkSession.builder \
    .appName("Local Spark to Hudi on Glue") \
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
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Create dummy data
data = [(1, "John Doe", "john@example.com", "2024-01-15"),
        (2, "Jane Smith", "jane@example.com", "2024-01-16")]
df = spark.createDataFrame(data, ["id", "name", "email", "date"]).withColumn("updated_at", current_timestamp())

# Refined Hudi Options
hudi_options = {
    'hoodie.table.name': TABLE_NAME,
    'hoodie.datasource.write.recordkey.field': 'id',
    'hoodie.datasource.write.partitionpath.field': 'date',
    'hoodie.datasource.write.precombine.field': 'updated_at',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.hive_style_partitioning': 'true',

    # --- THE CRITICAL FIXES FOR THE TRACEBACK ---
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.mode': 'glue',
    # This line tells Hudi WHICH class to use for the sync (prevents the HiveSyncTool error)
    'hoodie.datasource.hive_sync.metastore.client_class': 'org.apache.hudi.aws.sync.AwsGlueCatalogSyncClient',

    'hoodie.datasource.hive_sync.database': DATABASE_NAME,
    'hoodie.datasource.hive_sync.table': TABLE_NAME,
    'hoodie.datasource.hive_sync.partition_fields': 'date',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',

    # Required for Glue
    'hoodie.datasource.hive_sync.use_jdbc': 'false',

    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}


print(f"Writing Hudi table to {S3_PATH}...")
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(S3_PATH)

# Verify by querying the Catalog
print(f"Querying from Glue Catalog table: {DATABASE_NAME}.{TABLE_NAME}")
spark.sql(f"SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}").show()

spark.stop()