"# spark_learning"

import os
import sys
from datetime import datetime, date

os.environ["PYSPARK_PYTHON"] = "C:/Users/Raghava/AppData/Local/Programs/Python/Python311/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/Raghava/AppData/Local/Programs/Python/Python311/python.exe"

# Set Spark home

spark_home = "C:\\spark\\spark-3.5.3-bin-hadoop3"

# Add Spark to Python path

sys.path.insert(0, spark_home + "\\python")
sys.path.insert(0, spark_home + "\\python\\lib\\py4j-0.10.9.7-src.zip")

# Initialize Spark session

from pyspark.sql import SparkSession
from pyspark.sql import Row
spark = SparkSession.builder \
 .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
 .config("spark.hadoop.fs.file.disableCache", "true") \
 .config("spark.executor.memory", "4g") \
 .config("spark.driver.memory", "4g") \
 .config("spark.driver.maxResultSize", "2g") \
 .getOrCreate()

# Verify Spark session

print("Spark version: ", spark.sparkContext.version)
