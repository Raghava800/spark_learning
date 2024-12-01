Spark Learning Project
=====================================
Overview
------------
This project is designed to facilitate learning and experimentation with Apache Spark. It provides a basic setup for a Spark session, allowing users to explore various aspects of Spark.
Environment Setup
----------------------
Prerequisites
Python 3.11 (or higher)
Apache Spark 3.5.3 (or higher)
Configuration
Set the PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON environment variables to point to your Python executable.
Set the spark_home variable to the path where Apache Spark is installed.
Code
-----
Python
import os
import sys
from datetime import datetime, date

# Set environment variables
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
Usage
-----
Clone the repository to your local machine.
Ensure you have Apache Spark installed and configured correctly.
Run the code using python spark_learning.py (assuming you've named the file spark_learning.py).
Experiment with various Spark features and functionalities.