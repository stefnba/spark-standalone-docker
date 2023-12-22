import os

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("spark://spark-master:7077")  # type: ignore
    .appName("Testing Iceberg App with Glue")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") or ""
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or ""

print("1", AWS_ACCESS_KEY_ID)

spark.conf.set("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue.warehouse", "s3a://cloud-sdk/warehouse/")
spark.conf.set("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

spark.sql("CREATE NAMESPACE IF NOT EXISTS glue.app;")

spark.sql(
    """
CREATE TABLE glue.app.app (date Date, Close Float) USING iceberg PARTITIONED BY (YEAR(date));
"""
)
