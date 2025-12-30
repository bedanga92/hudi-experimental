from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from faker import Faker
from datetime import datetime
import pandas as pd
from hudi_glue_config import get_hudi_configuration
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if __name__ == "__main__":
    conf = get_hudi_configuration()

    spark = (
        SparkSession.builder
        .config(conf=conf)
        .master("local[*]")
        .appName("SampleDataFrame")
        # Removed .enableHiveSupport() to avoid local Derby metastore initialization
        .getOrCreate()
    )

    fake = Faker()
    data = []

    for i in range(5, 10):
        row = {
            "id": i + 1,
            "name": fake.name(),
            "city": fake.city(),
            "address": fake.address(),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "country": fake.country(),
            "state": fake.state(),
        }
        data.append(row)

    pdf = pd.DataFrame(data)
    df = spark.createDataFrame(pdf)
    df.show(truncate=False)

    additional_options = {
        "hoodie.table.name": "faker_hudi",
        "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "timestamp",
        "hoodie.datasource.write.partitionpath.field": "country",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        # Glue Sync Configuration
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.mode": "glue",  # Changed from "hms" to "glue"
        "hoodie.datasource.hive_sync.database": "hudi_experimental_db",
        "hoodie.datasource.hive_sync.table": "faker_hudi",
        "hoodie.datasource.hive_sync.partition_fields": "country",
        "hoodie.datasource.hive_sync.partition_extractor_class":
            "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.support_timestamp": "true",
        "path": "s3a://spark-hudi-experimental/target/tbl/faker_hudi",
        "hoodie.index.type": "GLOBAL_BLOOM",
        "hoodie.bloom.index.update.partition.path": "true",
    }

    (
        df.write
        .format("hudi")
        .options(**additional_options)
        .mode("append")
        .save()
    )

    print("✅ Data successfully written to Hudi table and synced with AWS Glue!")

    spark.stop()