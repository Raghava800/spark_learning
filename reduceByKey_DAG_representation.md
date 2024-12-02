**Spark Job and Task Execution Report**

**Program Code**

```python
rdd1 = spark.sparkContext.textFile(r"H:\..\practice\..\demo_sample.csv")
mapped_rdd = rdd1.map(lambda x: (x.split(",")[3], 1))
result_rdd = mapped_rdd.reduceByKey(lambda x, y: x + y)
result_rdd.collect()
```

**Job Overview**

| Property | Details |
|---|---|
| Job ID | Job0 |
| Status | SUCCEEDED |
| Submitted | December 2, 2024, 10:53:10 |
| Duration | 8 seconds |
| Stages Completed | 2 |

**Job Execution Stages**

**Stage 0: reduceByKey Operation**

* **Details:** Aggregates key-value pairs by summing their values.
* **Submitted:** December 2, 2024, 10:53:10
* **Duration:** 5 seconds
* **Tasks:** 2 (Succeeded/Total: 2/2)

**Stage 1: collect Operation**

* **Details:** Gathers the transformed RDD data into the driver.
* **Submitted:** December 2, 2024, 10:53:16
* **Duration:** 2 seconds
* **Tasks:** 2 (Succeeded/Total: 2/2)

**Task Execution Details**

| Task ID | Stage | Status | Duration | Input Data | Shuffle Write | Executor Host (Logs) |
|---|---|---|---|---|---|---|
| 0 | reduceByKey | SUCCESS | 5s | 818 B | - | driver (DESKTOP-GUAGO32-1) |
| 1 | reduceByKey | SUCCESS | 5s | 409 B | - | driver (DESKTOP-GUAGO32-2) |
| 2 | collect | SUCCESS | 2s | 411.0 B | 411.0 B | driver (DESKTOP-GUAGO32) |

**Job Summary**

The job successfully executed with two stages:

* Stage 0 handled the `reduceByKey` operation, summing the values for each key.
* Stage 1 gathered the aggregated results into the driver using the `collect` operation.

**Performance Highlights**

* Efficient Execution: Minimal garbage collection time and small input sizes ensured fast processing.
* Task Locality: All tasks executed locally on the driver (PROCESS_LOCAL).

**DAG Visualization**

```
      Stage 0 (reduceByKey)
           |
           v
      Stage 1 (collect)
```

**Code Breakdown:**

1. **Load the data:** Reads the CSV file into an RDD.
2. **Map the data:** Splits each line by commas, extracts the 4th element, and creates key-value pairs with 1 as the value.
3. **ReduceByKey:** Aggregates values with the same key by summing them.
4. **Collect (Optional):** Gathers the final results into the driver.