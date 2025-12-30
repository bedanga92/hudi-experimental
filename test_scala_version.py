from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
scala_version = spark.sparkContext._jvm.scala.util.Properties.versionString()
print(scala_version)
spark.stop()