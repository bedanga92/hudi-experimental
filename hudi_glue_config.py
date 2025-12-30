from pyspark import SparkConf


def get_hudi_configuration() -> SparkConf:
    conf = SparkConf()

    # --------------------------------------------------
    # Dependencies
    # --------------------------------------------------
    conf.set(
        "spark.jars.packages",
        ",".join([
            "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.1.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])
    )

    # --------------------------------------------------
    # Hudi
    # --------------------------------------------------
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")

    conf.set("spark.sql.extensions",
             "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog",
             "org.apache.spark.sql.hudi.catalog.HoodieCatalog")

    conf.set("spark.sql.hive.convertMetastoreParquet", "false")

    # --------------------------------------------------
    # Disable local Derby metastore
    # --------------------------------------------------
    conf.set("spark.sql.catalogImplementation", "in-memory")

    # --------------------------------------------------
    # S3A
    # --------------------------------------------------
    conf.set("spark.hadoop.fs.s3a.impl",
             "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

    # --------------------------------------------------
    # AWS Credentials (local)
    # --------------------------------------------------
    conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        ",".join([
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider"
        ])
    )
    conf.set("spark.hadoop.aws.ec2.metadata.disabled", "true")

    # --------------------------------------------------
    # Disable Hive Metastore (use Hudi's Glue sync instead)
    # --------------------------------------------------
    # Don't set hive.metastore.client.factory.class here
    # Hudi will handle Glue sync through its own mechanisms

    # --------------------------------------------------
    # Local driver networking
    # --------------------------------------------------
    conf.set("spark.driver.host", "localhost")
    conf.set("spark.driver.bindAddress", "127.0.0.1")

    return conf