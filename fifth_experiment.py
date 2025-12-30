from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from faker import Faker
import pandas as pd
from datetime import datetime
import os
import sys

# Standard Spark environment setup
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# --- CONFIGURATION ---
AWS_REGION = "us-east-1"  # Set your region
DATABASE_NAME = "hudi_experimental_db"
TABLE_NAME = "faker_dummy_tbl_hudi"
S3_PATH = f"s3a://spark-hudi-experimental/target/tbl/{TABLE_NAME}/"

# 1. Initialize Spark Session with Glue HMS Bridge
spark = SparkSession.builder \
    .appName("Local HMS Bridge to Glue") \
    .config("spark.master", "local[*]") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.jars.packages",
            "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
            "org.apache.hudi:hudi-aws-bundle:0.15.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("spark.hadoop.aws.region", AWS_REGION) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

fake = Faker()
data = []

for _ in range(1000):
    row = {
        "id": _ + 1,
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "country": fake.country(),
        "state": fake.state()

    }
    data.append(row)

pdf = pd.DataFrame(data)
# df = spark.createDataFrame(data, ["id", "name", "email", "date"]).withColumn("updated_at", current_timestamp())
df = spark.createDataFrame(pdf).withColumn("updated_at", current_timestamp())
df.show()

hudi_options = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.database.name": DATABASE_NAME,
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.partitionpath.field": "country",
    "hoodie.datasource.write.hive_style_partitioning": "true",

    # --- SYNC CONFIGURATION ---
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "glue",
    "hoodie.datasource.hive_sync.database": DATABASE_NAME,
    "hoodie.datasource.hive_sync.table": TABLE_NAME,
    "hoodie.datasource.hive_sync.partition_fields": "date",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",

    # --- CRITICAL NEW ADDITIONS ---
    # 1. Force the use of the Glue Sync Tool (prevents HiveSyncTool errors)
    "hoodie.datasource.hive_sync.sync_tool_classes": "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool",
    "hoodie.datasource.hive_sync.metastore.client_class": "org.apache.hudi.aws.sync.AwsGlueCatalogSyncClient",
    "hoodie.meta.sync.client.tool.class": "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool",

    # 2. Database & Table creation flags
    "hoodie.datasource.hive_sync.create_database": "true",
    "hoodie.datasource.hive_sync.table_properties": "classification=parquet",

    # 3. Error Handling
    "hoodie.datasource.hive_sync.ignore_errors": "false",

    # --- PERFORMANCE ---
    "hoodie.upsert.shuffle.parallelism": 10,
    "hoodie.insert.shuffle.parallelism": 10
}

print(f"Executing write and Glue sync for table: {DATABASE_NAME}.{TABLE_NAME}...")
df.write.format("hudi").options(**hudi_options).mode("append").save(S3_PATH)

# print("\nVerifying via Spark-Glue Bridge...")
# spark.sql(f"SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}").show()

# spark.stop()
