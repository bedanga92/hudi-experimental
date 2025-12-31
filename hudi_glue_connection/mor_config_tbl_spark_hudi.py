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
AWS_REGION = "us-east-1"
DATABASE_NAME = "hudi_experimental_db"
TABLE_NAME = "faker_dummy_tbl_hudi_mor"
S3_PATH = f"s3a://spark-hudi-experimental/target/tbl/{TABLE_NAME}/"

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("Hudi-MOR-Glue-Sync") \
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

# 2. Generate Dummy Data
fake = Faker()
data = []
for i in range(11,167):
    data.append({
        "id": i + 1,
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "country": fake.country(),
        "state": fake.state()
    })

# Note: "updated_at" is correctly added here
df = spark.createDataFrame(pd.DataFrame(data)).withColumn("updated_at", current_timestamp())

# 3. Hudi MOR Options
hudi_options = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.database.name": DATABASE_NAME,

    # [MOR CHANGE]: Set storage type to MERGE_ON_READ
    "hoodie.datasource.write.storage.type": "MERGE_ON_READ",

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

    # [MOR CHANGE]: Fixed mismatch. partition_fields MUST match partitionpath.field (country)
    "hoodie.datasource.hive_sync.partition_fields": "country",

    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",

    # --- GLUE SYNC TOOLS ---
    "hoodie.datasource.hive_sync.sync_tool_classes": "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool",
    "hoodie.meta.sync.client.tool.class": "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool",
    "hoodie.datasource.hive_sync.create_database": "true",

    # [MOR CHANGE]: Added classification for Glue catalog recognition
    "hoodie.datasource.hive_sync.table_properties": "classification=parquet",

    # [MOR CHANGE]: Performance & Compaction (Recommended for MOR)
    # Triggers compaction every 3 delta commits to merge log files into parquet
    "hoodie.compact.inline": "true",
    "hoodie.compact.inline.max.delta.commits": "3",
    "hoodie.upsert.shuffle.parallelism": 10,
    "hoodie.insert.shuffle.parallelism": 10
}

# 4. Write to S3 and Sync to Glue
print(f"Executing write for MOR table: {DATABASE_NAME}.{TABLE_NAME}...")
df.write.format("hudi").options(**hudi_options).mode("append").save(S3_PATH)

print("\nWrite complete. Checking Glue Catalog tables...")
# In MOR, Hudi creates _ro (Read Optimized) and _rt (Real Time) views automatically.
# spark.sql(f"SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}_rt LIMIT 5").show()