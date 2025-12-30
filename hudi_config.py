from pyspark import SparkConf


def get_hudi_configuration() -> SparkConf:
    conf = SparkConf()

    # Spark
    conf.set("spark.master", "local[*]")
    conf.set("spark.app.name", "HudiWindowsExample")
    conf.set("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.1.1")
    conf.set("spark.hadoop.fs.defaultFS", "file:///")
    conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    conf.set("spark.sql.warehouse.dir", "file:///D:/Dev/PycharmProjects/hudiExperimental/spark-warehouse")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    conf.set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    conf.set("spark.driver.host", "localhost")
    conf.set("spark.driver.bindAddress", "127.0.0.1")

    return conf
