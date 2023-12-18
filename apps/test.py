"""
This is a test file to check if the PySpark kernel is working.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()  # type: ignore

sample_data = [
    {"name": "John D.", "age": 30},
    {"name": "Alice  G.", "age": 25},
    {"name": "Bob T.", "age": 35},
    {"name": "Eve A.", "age": 28},
]

df = spark.createDataFrame(sample_data)

df.show()
