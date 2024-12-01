Here's you can find PySpark problems and their solutions:

---

# Spark Learning

This repository contains a setup guide and basic configuration for starting with Apache Spark using PySpark in Python. Follow the instructions below to set up your environment and run PySpark.

---

## Prerequisites

- Python 3.11 installed at:  
  `C:/Users/Raghava/AppData/Local/Programs/Python/Python311/python.exe`
- Apache Spark installed at:  
  `C:/spark/spark-3.5.3-bin-hadoop3`
- Required PySpark dependencies included in Spark installation.

---

## Setting Up the Environment

1. **Set Python Executable Paths**  
   Define the Python executable paths for PySpark and the driver:

   ```python
   import os

   os.environ["PYSPARK_PYTHON"] = "C:/Users/Raghava/AppData/Local/Programs/Python/Python311/python.exe"
   os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/Raghava/AppData/Local/Programs/Python/Python311/python.exe"
   ```

2. **Set Spark Home Directory**  
   Specify the location where Spark is installed:

   ```python
   spark_home = "C:\\spark\\spark-3.5.3-bin-hadoop3"
   ```

3. **Add Spark to Python Path**  
   Add Spark's Python library and Py4J to the system path:

   ```python
   import sys

   sys.path.insert(0, spark_home + "\\python")
   sys.path.insert(0, spark_home + "\\python\\lib\\py4j-0.10.9.7-src.zip")
   ```

---

## Initialize a Spark Session

To start using PySpark, create a `SparkSession` with custom configurations:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.file.disableCache", "true") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()
```

---

## Verifying the Spark Setup

To verify that Spark has been set up correctly, print the Spark version:

```python
print("Spark version: ", spark.sparkContext.version)
```

---

## Example Code Snippet

The following code demonstrates how to use PySpark's DataFrame API:

```python
from pyspark.sql import Row

# Create a simple DataFrame
data = [Row(id=1, name="Alice"), Row(id=2, name="Bob")]
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()
```

---

## Troubleshooting

- Ensure that the paths for Python and Spark are correctly set in the script.
- Verify that the `spark_home` directory contains the necessary Spark binaries and libraries.
- Check the compatibility of the Python version with the Spark version.

---

## References

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

---

This `README.md` now provides a clear and structured guide for setting up and running Apache Spark with PySpark. Let me know if you'd like further refinements!
