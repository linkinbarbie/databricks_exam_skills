---
name: apache-spark
description: Master Apache Spark for certification exams - covers architecture, RDDs, DataFrames, Spark SQL, Catalyst optimizer, performance tuning, and Structured Streaming
version: 1.0.0
author: Custom
tags: [Apache Spark, PySpark, Spark SQL, DataFrames, Certification, Data Engineering, Performance Tuning]
dependencies: []
---

# Apache Spark Mastery

## When to Use This Skill

Use this skill when you need to:
- **Prepare for Spark certification exams** (Databricks Certified Associate/Professional)
- **Understand Spark architecture** (driver, executors, cluster managers)
- **Work with RDDs, DataFrames, and Datasets**
- **Optimize Spark jobs** (partitioning, caching, broadcast joins, shuffle)
- **Write Spark SQL and DataFrame transformations**
- **Debug and tune Spark performance**

## Spark Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SPARK APPLICATION                            │
│                                                                     │
│  ┌──────────────┐         ┌────────────────────────────────────┐    │
│  │   DRIVER     │         │        CLUSTER MANAGER             │    │
│  │              │◄───────►│  (Standalone / YARN / Mesos / K8s) │    │
│  │ SparkContext │         └────────────────────────────────────┘    │
│  │ SparkSession │                        │                          │
│  │ DAG Scheduler│         ┌──────────────┼──────────────┐          │
│  │ Task Sched.  │         ▼              ▼              ▼          │
│  └──────────────┘   ┌──────────┐   ┌──────────┐   ┌──────────┐    │
│                     │ EXECUTOR │   │ EXECUTOR │   │ EXECUTOR │    │
│                     │          │   │          │   │          │    │
│                     │ ┌──────┐ │   │ ┌──────┐ │   │ ┌──────┐ │    │
│                     │ │Task 1│ │   │ │Task 3│ │   │ │Task 5│ │    │
│                     │ │Task 2│ │   │ │Task 4│ │   │ │Task 6│ │    │
│                     │ └──────┘ │   │ └──────┘ │   │ └──────┘ │    │
│                     │ [Cache]  │   │ [Cache]  │   │ [Cache]  │    │
│                     └──────────┘   └──────────┘   └──────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | Role |
|-----------|------|
| **Driver** | Runs main program, creates SparkContext/SparkSession, builds DAG, schedules tasks |
| **Executor** | JVM process on worker node, runs tasks, stores cached data |
| **Cluster Manager** | Allocates resources (Standalone, YARN, Mesos, Kubernetes) |
| **Task** | Smallest unit of work, runs on one partition |
| **Stage** | Set of tasks that can run in parallel (bounded by shuffles) |
| **Job** | Triggered by an action, consists of one or more stages |

### Execution Flow

```
Action called → Job created → DAG built → Stages split at shuffle boundaries
→ Tasks created (1 per partition per stage) → Tasks sent to executors → Results returned
```

## Core Abstractions

### RDD vs DataFrame vs Dataset

| Feature | RDD | DataFrame | Dataset |
|---------|-----|-----------|---------|
| **Type Safety** | Compile-time | Runtime | Compile-time |
| **Optimization** | None (manual) | Catalyst + Tungsten | Catalyst + Tungsten |
| **API** | Functional (map, filter) | Declarative (select, where) | Both |
| **Schema** | No schema | Has schema | Has schema |
| **Language** | Python, Scala, Java | Python, Scala, Java, R | Scala, Java only |
| **Serialization** | Java/Kryo | Tungsten binary | Tungsten binary |
| **When to use** | Low-level control | Most use cases | Type-safe Scala/Java |

### SparkSession (Entry Point)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# SparkSession includes SparkContext
sc = spark.sparkContext
```

### Creating DataFrames

```python
# From data
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

# From file
df = spark.read.format("parquet").load("/data/users/")
df = spark.read.csv("/data/users.csv", header=True, inferSchema=True)
df = spark.read.json("/data/events.json")

# From table
df = spark.table("db.users")

# From SQL
df = spark.sql("SELECT * FROM db.users WHERE active = true")
```

## Transformations vs Actions

### Transformations (Lazy - return new DataFrame/RDD)

**Narrow (no shuffle):**
```python
df.select("name", "age")           # Select columns
df.filter(df.age > 25)             # Filter rows
df.withColumn("age2", df.age * 2)  # Add/modify column
df.drop("temp_col")                # Drop column
df.distinct()                      # Remove duplicates (can cause shuffle)
df.coalesce(4)                     # Reduce partitions (no shuffle)
df.map(lambda x: x.upper())       # RDD map
```

**Wide (causes shuffle):**
```python
df.groupBy("dept").count()                # Group by
df.orderBy("salary")                      # Sort
df.join(other_df, "id")                   # Join
df.repartition(10)                        # Repartition (shuffle)
df.repartition("country")                 # Partition by column
df.reduceByKey(lambda a, b: a + b)        # RDD reduce by key
```

### Actions (Eager - trigger computation)

```python
df.show()                    # Display rows
df.collect()                 # Return all rows as list
df.take(5)                   # Return first 5 rows
df.first()                   # Return first row
df.count()                   # Count rows
df.describe().show()         # Summary statistics
df.write.parquet("/output")  # Write to storage
df.foreach(func)             # Apply function to each row
```

**Key Rule:** Nothing executes until an action is called (lazy evaluation).

## Spark SQL

### DataFrame API vs SQL

```python
# DataFrame API
result = (df
    .filter(df.age > 25)
    .groupBy("department")
    .agg(
        F.count("*").alias("count"),
        F.avg("salary").alias("avg_salary")
    )
    .orderBy(F.desc("avg_salary")))

# Equivalent SQL
df.createOrReplaceTempView("employees")
result = spark.sql("""
    SELECT department, COUNT(*) AS count, AVG(salary) AS avg_salary
    FROM employees
    WHERE age > 25
    GROUP BY department
    ORDER BY avg_salary DESC
""")
```

### Common Functions (pyspark.sql.functions)

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# String functions
F.upper("name"), F.lower("name"), F.trim("name")
F.concat(F.col("first"), F.lit(" "), F.col("last"))
F.substring("name", 1, 3)
F.regexp_replace("text", r"\d+", "NUM")
F.split("csv_col", ",")

# Date/Time functions
F.current_date(), F.current_timestamp()
F.datediff("end_date", "start_date")
F.date_add("date", 7), F.date_sub("date", 7)
F.year("date"), F.month("date"), F.dayofweek("date")
F.date_format("date", "yyyy-MM-dd")
F.to_date("string_col", "MM/dd/yyyy")

# Aggregate functions
F.count("col"), F.countDistinct("col")
F.sum("col"), F.avg("col"), F.min("col"), F.max("col")
F.collect_list("col"), F.collect_set("col")

# Conditional
F.when(F.col("age") > 18, "adult").otherwise("minor")
F.coalesce("col1", "col2", F.lit("default"))

# Null handling
F.col("x").isNull(), F.col("x").isNotNull()
df.na.fill(0), df.na.drop()
```

### Window Functions

```python
from pyspark.sql.window import Window

# Define window
w = Window.partitionBy("department").orderBy("salary")

# Ranking
df.withColumn("row_num", F.row_number().over(w))
df.withColumn("rank", F.rank().over(w))
df.withColumn("dense_rank", F.dense_rank().over(w))

# Analytic
df.withColumn("prev_salary", F.lag("salary", 1).over(w))
df.withColumn("next_salary", F.lead("salary", 1).over(w))

# Aggregate over window
w_unbounded = Window.partitionBy("dept").orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", F.sum("amount").over(w_unbounded))
```

## Joins

```python
# Inner join (default)
df1.join(df2, "id")
df1.join(df2, df1.id == df2.user_id)

# Left / Right / Full outer
df1.join(df2, "id", "left")
df1.join(df2, "id", "right")
df1.join(df2, "id", "outer")

# Left semi (exists in right)
df1.join(df2, "id", "left_semi")

# Left anti (not exists in right)
df1.join(df2, "id", "left_anti")

# Cross join
df1.crossJoin(df2)

# Broadcast join (small table fits in memory)
from pyspark.sql.functions import broadcast
df1.join(broadcast(df2), "id")
```

### Join Strategies

| Strategy | When Used | Best For |
|----------|-----------|----------|
| **Broadcast Hash Join** | Small table < 10MB (configurable) | Small + Large table |
| **Sort Merge Join** | Default for large tables | Two large tables |
| **Shuffle Hash Join** | One side fits in memory after partition | Medium + Large |
| **Cartesian Product** | Cross join or no join condition | Avoid if possible |

```python
# Force broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10MB

# Disable broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

## Catalyst Optimizer

```
┌─────────┐    ┌──────────┐    ┌───────────┐    ┌──────────┐    ┌──────────┐
│ Unresolved│──►│ Logical  │──►│ Optimized │──►│ Physical │──►│ Selected │
│   Plan   │   │  Plan    │   │  Plan     │   │  Plans   │   │   Plan   │
└─────────┘    └──────────┘    └───────────┘    └──────────┘    └──────────┘
   Parsing      Analysis      Optimization    Physical       Cost-Based
                (resolve      (predicate     Planning       Selection
                 names,       pushdown,
                 types)       constant
                              folding,
                              column pruning)
```

**Key Optimizations:**
- **Predicate Pushdown**: Filters pushed close to data source
- **Column Pruning**: Only read needed columns
- **Constant Folding**: Evaluate constant expressions at compile time
- **Join Reordering**: Optimize join order based on table sizes
- **Whole-Stage Code Generation**: Compile to JVM bytecode (Tungsten)

```python
# View query plan
df.explain()           # Simple physical plan
df.explain(True)       # All plans (parsed, analyzed, optimized, physical)
df.explain("formatted") # Formatted output
```

## Partitioning and Shuffling

### Partitioning

```python
# Check current partitions
df.rdd.getNumPartitions()

# Repartition (causes shuffle - use for increasing partitions)
df.repartition(200)
df.repartition("country")  # Hash partition by column
df.repartition(200, "country")

# Coalesce (no shuffle - use for decreasing partitions)
df.coalesce(10)
```

**Rules of Thumb:**
- Target 128MB per partition
- 2-4 partitions per CPU core
- Use `spark.sql.shuffle.partitions` for shuffle operations (default 200)

### Shuffle

A shuffle occurs when data must be redistributed across partitions:
- `groupBy`, `join`, `orderBy`, `repartition`, `distinct`
- `reduceByKey`, `aggregateByKey` (RDD)

**Shuffle is expensive because it involves:**
1. Writing intermediate data to disk
2. Network transfer between executors
3. Reading and deserializing on receiving end

### Adaptive Query Execution (AQE)

```python
# Enable AQE (default in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", True)

# AQE features:
# 1. Coalescing shuffle partitions (reduces small partitions)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)

# 2. Converting sort-merge join to broadcast join at runtime
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", True)

# 3. Skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
```

## Caching and Persistence

```python
# Cache (memory only, deserialized)
df.cache()

# Persist with storage level
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_ONLY)          # Default for cache()
df.persist(StorageLevel.MEMORY_AND_DISK)      # Spill to disk
df.persist(StorageLevel.DISK_ONLY)            # Disk only
df.persist(StorageLevel.MEMORY_ONLY_SER)      # Serialized (less memory)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # Serialized + disk spillover

# Unpersist
df.unpersist()

# Check if cached
df.is_cached
```

**When to Cache:**
- DataFrame used multiple times in same job
- After expensive transformations (joins, aggregations)
- Iterative algorithms (ML training)

**When NOT to Cache:**
- DataFrame used only once
- Data too large to fit in memory
- After simple filters on already cached data

## Performance Tuning Checklist

### 1. Minimize Shuffles
```python
# Use broadcast joins for small tables
df1.join(broadcast(small_df), "id")

# Use coalesce instead of repartition to reduce partitions
df.coalesce(10)  # No shuffle

# Pre-partition data
df.repartition("join_key").write.partitionBy("join_key").parquet("/data/")
```

### 2. Optimize Serialization
```python
# Use Kryo serializer (faster than Java)
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# Prefer DataFrames over RDDs (Tungsten binary format)
```

### 3. Tune Memory
```python
# Executor memory layout:
# spark.executor.memory = [Execution Memory | Storage Memory | User Memory | Reserved]

# Unified memory management (default)
spark.conf.set("spark.memory.fraction", 0.6)          # 60% for execution + storage
spark.conf.set("spark.memory.storageFraction", 0.5)    # 50% of unified for storage
```

### 4. Key Configuration

| Config | Default | Description |
|--------|---------|-------------|
| `spark.sql.shuffle.partitions` | 200 | Partitions after shuffle |
| `spark.default.parallelism` | Total cores | Default RDD partitions |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | Auto-broadcast threshold |
| `spark.executor.memory` | 1g | Memory per executor |
| `spark.executor.cores` | 1 | Cores per executor |
| `spark.driver.memory` | 1g | Driver memory |
| `spark.sql.adaptive.enabled` | true (3.2+) | Enable AQE |

## Structured Streaming (Quick Reference)

```python
# Read stream
stream_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host:9092")
    .option("subscribe", "topic")
    .load())

# Transform
result = (stream_df
    .selectExpr("CAST(value AS STRING)")
    .groupBy(F.window("timestamp", "10 minutes"), "word")
    .count())

# Write stream
query = (result.writeStream
    .format("delta")
    .outputMode("append")       # append / complete / update
    .option("checkpointLocation", "/checkpoint/")
    .trigger(processingTime="1 minute")
    .start())

query.awaitTermination()
```

**Output Modes:**
| Mode | Description | Use With |
|------|-------------|----------|
| **append** | Only new rows | No aggregations, or watermarked |
| **complete** | All rows every trigger | Aggregations |
| **update** | Only changed rows | Aggregations |

**Triggers:**
| Trigger | Description |
|---------|-------------|
| `processingTime="10 seconds"` | Micro-batch every 10s |
| `once=True` | Single micro-batch then stop |
| `availableNow=True` | Process all available, then stop |
| `continuous="1 second"` | Continuous processing (experimental) |

See `references/structured-streaming.md` for watermarks, late data, and state management.

## Deployment Modes

| Mode | Driver Location | Use Case |
|------|-----------------|----------|
| **Client** | Local machine | Interactive (notebooks, spark-shell) |
| **Cluster** | Inside cluster | Production jobs (spark-submit) |
| **Local** | Local machine | Testing (`local[*]`) |

```bash
# Submit in cluster mode
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-memory 4g \
  --executor-cores 4 \
  my_app.py
```

## Certification Exam Tips

### Databricks Certified Associate Developer for Apache Spark

**Focus areas:**
1. Spark architecture and execution model (20%)
2. DataFrame API and transformations (30%)
3. Spark SQL and functions (25%)
4. Performance and tuning (15%)
5. Structured Streaming basics (10%)

**Must-Know Concepts:**
- Lazy evaluation: transformations vs actions
- Narrow vs wide transformations (shuffle boundary)
- DataFrame API: select, filter, groupBy, join, window functions
- Catalyst optimizer and query plans
- Partitioning strategies (repartition vs coalesce)
- Caching: when and how
- Broadcast joins
- Output modes in streaming

### Databricks Certified Professional

**Additional topics:**
1. Advanced performance tuning (AQE, skew handling)
2. Custom partitioners and bucketing
3. UDFs and their performance implications
4. Complex streaming (watermarks, state, joins)
5. Spark internals (DAG, stages, tasks, shuffle)

## Quick Reference: Common Patterns

```python
# Read → Transform → Write
(spark.read.parquet("/input")
    .filter(F.col("status") == "active")
    .groupBy("category").agg(F.sum("amount").alias("total"))
    .write.mode("overwrite").parquet("/output"))

# Deduplication
df.dropDuplicates(["id"])  # Keep first occurrence
df.withColumn("rn", F.row_number().over(
    Window.partitionBy("id").orderBy(F.desc("updated_at"))
)).filter(F.col("rn") == 1).drop("rn")

# Pivot
df.groupBy("year").pivot("quarter").sum("revenue")

# Unpivot (stack)
df.selectExpr("id", "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (quarter, revenue)")

# Schema handling
df.printSchema()
df.schema
df.dtypes
df.columns
```

## See Also

- `references/spark-architecture.md` - Deep dive on driver, executors, DAG, stages
- `references/rdd-dataframe-dataset.md` - Core abstractions in detail
- `references/transformations-actions.md` - Complete list with narrow/wide classification
- `references/performance-tuning.md` - Memory, shuffle, skew, and optimization
- `references/spark-sql-catalyst.md` - Catalyst optimizer and Tungsten engine
- `references/structured-streaming.md` - Watermarks, state, stream-stream joins
- `references/exam-questions.md` - Practice questions for certification
