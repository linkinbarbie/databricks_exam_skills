# Apache Spark Exam Practice Questions

## Architecture & Execution Model

### Q1: Driver and Executors
**Which component is responsible for creating the DAG and scheduling tasks?**

A) Executor
B) Cluster Manager
C) Worker Node
D) Driver

<details>
<summary>Answer</summary>

**D) Driver**

The driver runs the main program, creates the SparkContext/SparkSession, converts user code into a DAG of stages and tasks, and uses the DAG Scheduler and Task Scheduler to assign tasks to executors. The cluster manager only allocates resources.
</details>

### Q2: Deployment Modes
**In cluster deploy mode, where does the driver run?**

A) On the client machine that submitted the job
B) On one of the worker nodes in the cluster
C) On the cluster manager node
D) On all executor nodes

<details>
<summary>Answer</summary>

**B) On one of the worker nodes in the cluster**

In cluster mode, the driver runs inside the cluster on a worker node. In client mode, the driver runs on the submitting machine. This is a common exam question.
</details>

### Q3: Stages and Tasks
**What determines stage boundaries in a Spark job?**

A) Actions
B) Narrow transformations
C) Wide transformations (shuffles)
D) The number of executors

<details>
<summary>Answer</summary>

**C) Wide transformations (shuffles)**

Stages are created at shuffle boundaries. Wide transformations like `groupBy`, `join`, `orderBy`, and `repartition` cause shuffles and create new stages. Narrow transformations within a stage are pipelined.
</details>

### Q4: Number of Tasks
**If a DataFrame has 100 partitions and goes through 3 stages, how many total tasks are created?**

A) 100
B) 200
C) 300
D) It depends on the transformations in each stage

<details>
<summary>Answer</summary>

**D) It depends on the transformations in each stage**

The number of tasks per stage equals the number of partitions for that stage. Shuffles may change the number of partitions (controlled by `spark.sql.shuffle.partitions`). So Stage 0 might have 100 tasks, but Stage 1 might have 200 tasks (default shuffle partitions).
</details>

## Transformations & Actions

### Q5: Lazy Evaluation
**Which of the following triggers execution in Spark?**

A) `df.filter(df.age > 25)`
B) `df.groupBy("dept").count()`
C) `df.select("name")`
D) `df.count()`

<details>
<summary>Answer</summary>

**D) `df.count()`**

`count()` is an action that triggers execution. `filter`, `groupBy`, and `select` are transformations that are lazy - they build a DAG but don't execute until an action is called.

Note: `groupBy("dept").count()` looks like it might be an action because of `.count()`, but `groupBy().count()` returns a DataFrame (transformation). Only standalone `.count()` on a DataFrame is an action.
</details>

### Q6: Narrow vs Wide
**Which transformation does NOT cause a shuffle?**

A) `df.groupBy("dept").sum("salary")`
B) `df.join(other_df, "id")`
C) `df.coalesce(10)`
D) `df.repartition(100)`

<details>
<summary>Answer</summary>

**C) `df.coalesce(10)`**

`coalesce` reduces partitions by combining partitions on the same executor without a shuffle. `groupBy`, `join`, and `repartition` all cause shuffles.
</details>

### Q7: Repartition vs Coalesce
**You have a DataFrame with 1000 partitions and want to reduce it to 10 before writing. Which is most efficient?**

A) `df.repartition(10)`
B) `df.coalesce(10)`
C) `df.repartition(10, "id")`
D) Both A and B are equally efficient

<details>
<summary>Answer</summary>

**B) `df.coalesce(10)`**

`coalesce` reduces partitions without a full shuffle - it simply combines existing partitions. `repartition` always performs a full shuffle. For reducing the number of partitions, `coalesce` is always more efficient.
</details>

## DataFrames & Spark SQL

### Q8: DataFrame Creation
**What happens when you read a CSV file with `inferSchema=False` (default)?**

A) All columns are read as StringType
B) Spark infers types from the data
C) An error is thrown
D) Numeric columns are read as IntegerType

<details>
<summary>Answer</summary>

**A) All columns are read as StringType**

By default, `inferSchema=False` for CSV, which means all columns are treated as strings. Set `inferSchema=True` to have Spark scan the data and infer types (requires an extra pass over the data). Better practice: provide an explicit schema.
</details>

### Q9: Window Functions
**What is the difference between RANK() and DENSE_RANK()?**

A) RANK() doesn't handle ties, DENSE_RANK() does
B) RANK() skips numbers after ties, DENSE_RANK() doesn't
C) DENSE_RANK() is faster than RANK()
D) They are identical

<details>
<summary>Answer</summary>

**B) RANK() skips numbers after ties, DENSE_RANK() doesn't**

Example with values [100, 90, 90, 80]:
- RANK(): 1, 2, 2, 4 (skips 3)
- DENSE_RANK(): 1, 2, 2, 3 (no gaps)
- ROW_NUMBER(): 1, 2, 3, 4 (no ties)
</details>

### Q10: Join Types
**Which join returns only the rows from the left DataFrame that have a matching key in the right DataFrame, without including any columns from the right DataFrame?**

A) Inner join
B) Left outer join
C) Left semi join
D) Left anti join

<details>
<summary>Answer</summary>

**C) Left semi join**

- `left_semi`: Returns left rows that match right (no right columns) - like `WHERE EXISTS`
- `left_anti`: Returns left rows that do NOT match right - like `WHERE NOT EXISTS`
- `inner`: Returns matching rows from both sides
- `left outer`: Returns all left rows plus matching right rows (nulls for no match)
</details>

## Performance & Optimization

### Q11: Broadcast Join
**When should you use a broadcast join?**

A) When both tables are large
B) When one table is small enough to fit in executor memory
C) When joining on multiple columns
D) When using left outer joins

<details>
<summary>Answer</summary>

**B) When one table is small enough to fit in executor memory**

Broadcast joins send the small table to all executors, allowing a local hash join without shuffling the large table. Default threshold: 10MB (`spark.sql.autoBroadcastJoinThreshold`). Regardless of join type or number of columns.
</details>

### Q12: Caching
**After calling `df.cache()`, when is the data actually cached?**

A) Immediately when `cache()` is called
B) When the next transformation is called
C) When the next action is called
D) When `persist()` is called

<details>
<summary>Answer</summary>

**C) When the next action is called**

`cache()` is lazy - it marks the DataFrame for caching but doesn't compute anything. The data is actually cached when the next action (like `count()`, `show()`, or `collect()`) triggers execution.
</details>

### Q13: UDF Performance
**Which type of user-defined function has the best performance in PySpark?**

A) Python UDF (`@udf`)
B) Pandas UDF (`@pandas_udf`)
C) Built-in functions (`pyspark.sql.functions`)
D) All have the same performance

<details>
<summary>Answer</summary>

**C) Built-in functions (`pyspark.sql.functions`)**

Performance ranking:
1. **Built-in functions** (fastest - run in JVM, Catalyst-optimized)
2. **Pandas UDFs** (vectorized via Apache Arrow)
3. **Python UDFs** (slowest - data serialized between JVM and Python per row)

Always prefer built-in functions. Use Pandas UDFs when custom logic is needed.
</details>

### Q14: Catalyst Optimizer
**Which optimization does Catalyst perform automatically?**

A) Caching frequently used DataFrames
B) Predicate pushdown (filtering before joins)
C) Choosing the number of executors
D) Converting Python UDFs to built-in functions

<details>
<summary>Answer</summary>

**B) Predicate pushdown (filtering before joins)**

Catalyst automatically pushes filters closer to the data source, prunes unnecessary columns, folds constants, and reorders joins. It does NOT auto-cache, choose executors, or optimize UDFs.
</details>

### Q15: Shuffle Partitions
**A query produces very small output files after a groupBy. What is the most likely cause?**

A) Too few executors
B) `spark.sql.shuffle.partitions` is too high
C) Data is skewed
D) Caching is not enabled

<details>
<summary>Answer</summary>

**B) `spark.sql.shuffle.partitions` is too high**

The default is 200. If your data after groupBy is small, 200 partitions means 200 tiny files. Solutions:
1. Reduce `spark.sql.shuffle.partitions` (e.g., to 20)
2. Enable AQE: `spark.sql.adaptive.enabled = true` (auto-coalesces small partitions)
3. Use `coalesce()` before writing
</details>

## Structured Streaming

### Q16: Output Modes
**Which output mode should you use with a streaming aggregation query that writes to a file sink?**

A) append
B) complete
C) update
D) append with watermark

<details>
<summary>Answer</summary>

**D) append with watermark**

File sinks only support `append` mode. For streaming aggregations in append mode, a watermark is required (so Spark knows when to finalize and output results). Without a watermark, `append` mode with aggregations throws an error.
</details>

### Q17: Watermarks
**What happens to data that arrives after the watermark threshold?**

A) It is buffered until the next trigger
B) It is dropped and not included in results
C) It causes an error
D) It is always included in results

<details>
<summary>Answer</summary>

**B) It is dropped and not included in results**

The watermark defines the maximum acceptable lateness. Data arriving later than `max_event_time - watermark_delay` is considered too late and is dropped. This also allows Spark to clean up old state.
</details>

### Q18: Checkpoints
**What is stored in a Structured Streaming checkpoint?**

A) Only the processed data
B) Offsets, state data, and commit log
C) Only the query plan
D) The entire input dataset

<details>
<summary>Answer</summary>

**B) Offsets, state data, and commit log**

Checkpoints store: (1) source offsets - which data has been read, (2) state data - aggregation state, join state, etc., (3) commit log - which batches have completed. This enables exactly-once processing and recovery from failures.
</details>

### Q19: Trigger Types
**Which trigger processes all available data and then stops the query?**

A) `trigger(processingTime="0 seconds")`
B) `trigger(once=True)`
C) `trigger(availableNow=True)`
D) Both B and C

<details>
<summary>Answer</summary>

**D) Both B and C**

Both process available data and stop. However, `once=True` is deprecated in favor of `availableNow=True`. The key difference: `availableNow=True` may process data in multiple micro-batches for better scalability, while `once=True` uses a single batch.
</details>

## Memory & Configuration

### Q20: Executor Memory
**In the unified memory management model, what happens when execution memory needs more space?**

A) An OutOfMemoryError is thrown
B) Execution can evict storage memory
C) Storage memory can evict execution memory
D) The executor is terminated

<details>
<summary>Answer</summary>

**B) Execution can evict storage memory**

In unified memory management, execution and storage share a pool. Execution has priority: it can evict cached data from storage memory. Storage cannot evict execution memory. This prevents OOM errors for shuffle/sort operations at the cost of losing cached data.
</details>

## Scenario-Based Questions

### Q21: Debugging Slow Joins
**Your Spark job has a join between two large tables that is running very slowly. The Spark UI shows one task taking 10x longer than others. What is the most likely issue and solution?**

<details>
<summary>Answer</summary>

**Data Skew** - One key has significantly more data than others, causing one partition (and task) to be much larger.

Solutions:
1. **AQE Skew Join**: `spark.sql.adaptive.skewJoin.enabled = true`
2. **Salting**: Add random prefix to skewed key, explode the lookup table
3. **Broadcast**: If one table is small enough, use `broadcast()`
4. **Isolate**: Process the hot key separately
</details>

### Q22: Small Files Problem
**Your streaming job writes 200 small Parquet files every trigger interval. How do you reduce the number of output files?**

<details>
<summary>Answer</summary>

Options:
1. **Reduce shuffle partitions**: `spark.sql.shuffle.partitions = 10`
2. **Use `coalesce()`** before writing: `df.coalesce(1).writeStream...`
3. **Enable AQE**: Auto-coalesces small partitions
4. **Increase trigger interval**: Process more data per batch
5. **Post-process compaction**: Run `OPTIMIZE` on Delta tables
</details>

### Q23: OOM Error
**Your driver keeps running out of memory. Which of these could cause it?**

A) `df.collect()` on a large DataFrame
B) Broadcasting a very large table
C) Calling `df.count()`
D) Both A and B

<details>
<summary>Answer</summary>

**D) Both A and B**

- `collect()` brings ALL data to the driver - if the DataFrame is large, the driver OOMs
- `broadcast()` collects the table to the driver first, then sends to executors
- `count()` only returns a single number to the driver, so it's safe

Fix: Increase `spark.driver.memory`, avoid `collect()` on large data, ensure broadcast tables are actually small.
</details>

### Q24: Reading Data
**You need to read a large directory of JSON files where the schema changes over time (new fields added). What approach handles this best?**

<details>
<summary>Answer</summary>

Options:
1. **Schema evolution with `mergeSchema`**:
   ```python
   spark.read.option("mergeSchema", True).json("/data/")
   ```
2. **Explicit schema with `mode("PERMISSIVE")`**:
   ```python
   spark.read.schema(base_schema).option("mode", "PERMISSIVE").json("/data/")
   ```
3. **Auto Loader (Databricks)**:
   ```python
   spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
       .load("/data/")
   ```

Best practice: Define a schema and use `PERMISSIVE` mode with `columnNameOfCorruptRecord` to capture unparseable records.
</details>

### Q25: Adaptive Query Execution
**Which of the following are features of AQE (Adaptive Query Execution)? Select all that apply.**

A) Coalescing post-shuffle partitions
B) Converting sort-merge join to broadcast join at runtime
C) Optimizing skewed joins
D) Automatically caching DataFrames

<details>
<summary>Answer</summary>

**A, B, C**

AQE's three main features:
1. **Coalesce shuffle partitions**: Merges small partitions after shuffle
2. **Convert to broadcast join**: If one side of join is smaller than expected after filtering
3. **Skew join optimization**: Splits skewed partitions

AQE does NOT auto-cache DataFrames.
</details>
