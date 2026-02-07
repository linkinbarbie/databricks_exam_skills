# Spark Performance Tuning

## The 5 Pillars of Spark Performance

1. **Minimize shuffles**
2. **Optimize partitioning**
3. **Use caching wisely**
4. **Tune memory and configuration**
5. **Write efficient code**

## 1. Minimize Shuffles

Shuffles are the most expensive operation in Spark. They involve:
- Serializing data on the sending side
- Writing to disk
- Transferring across the network
- Reading and deserializing on the receiving side

### Avoid Unnecessary Shuffles

```python
# BAD: Two shuffles (groupBy + orderBy)
df.groupBy("dept").count().orderBy("count")

# BETTER: Combine operations when possible
# Use window functions instead of groupBy + join
w = Window.partitionBy("dept")
df.withColumn("dept_count", F.count("*").over(w))

# BAD: groupByKey shuffles ALL data
rdd.groupByKey().mapValues(sum)

# BETTER: reduceByKey pre-aggregates before shuffle
rdd.reduceByKey(lambda a, b: a + b)
```

### Broadcast Joins

When one DataFrame is small enough to fit in executor memory:

```python
from pyspark.sql.functions import broadcast

# Explicit broadcast (recommended)
large_df.join(broadcast(small_df), "id")

# Auto-broadcast threshold (default 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50MB

# Disable auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

**How broadcast works:**
1. Small table collected to driver
2. Serialized and sent to every executor
3. Each executor does a local hash join (no shuffle!)

### Pre-Partitioning

If you join on the same key repeatedly:

```python
# Partition both DataFrames by join key
df1 = df1.repartition(200, "user_id")
df2 = df2.repartition(200, "user_id")

# Subsequent joins on user_id won't need a full shuffle
result = df1.join(df2, "user_id")
```

### Bucketing

For repeated joins on the same column:

```python
# Write bucketed table
(df.write
    .bucketBy(256, "user_id")
    .sortBy("user_id")
    .saveAsTable("users_bucketed"))

# Joins on bucketed tables skip the shuffle
a = spark.table("users_bucketed")
b = spark.table("orders_bucketed")  # Also bucketed by user_id
result = a.join(b, "user_id")  # No shuffle!
```

## 2. Optimize Partitioning

### Right-Sizing Partitions

```python
# Check current partitions
df.rdd.getNumPartitions()

# Guidelines:
# - Target: 128MB per partition
# - 2-4 partitions per CPU core
# - Too few partitions: OOM, poor parallelism
# - Too many partitions: overhead, small file problem
```

### Shuffle Partitions

```python
# Default is 200 - adjust based on data size
spark.conf.set("spark.sql.shuffle.partitions", 200)

# For small data: reduce to avoid too many small partitions
spark.conf.set("spark.sql.shuffle.partitions", 20)

# For large data: increase
spark.conf.set("spark.sql.shuffle.partitions", 2000)

# BEST: Use Adaptive Query Execution (auto-tunes)
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
```

### Repartition vs Coalesce

```python
# Repartition: full shuffle, can increase or decrease partitions
df.repartition(200)          # Hash repartition
df.repartition("col")       # Partition by column
df.repartition(200, "col")  # Both

# Coalesce: NO shuffle, can ONLY decrease partitions
df.coalesce(10)  # Combines partitions on same executor

# When to use which:
# - Increasing partitions → repartition (must shuffle)
# - Decreasing partitions → coalesce (no shuffle, faster)
# - Partitioning by column → repartition (must shuffle)
# - Before write to control file count → coalesce
```

### Handling Data Skew

Data skew = some partitions much larger than others:

```python
# Detect skew: check partition sizes
df.groupBy(F.spark_partition_id().alias("partition")).count().show()

# Solution 1: Salting
# Add random prefix to skewed key, join, then remove
df_salted = df.withColumn("salt", F.floor(F.rand() * 10))
df_salted = df_salted.withColumn(
    "salted_key", F.concat("key", F.lit("_"), "salt")
)

# Solution 2: AQE Skew Join (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Solution 3: Broadcast if one side is small
large_df.join(broadcast(small_df), "key")

# Solution 4: Isolate skewed key
skewed = df.filter(df.key == "hot_key")
normal = df.filter(df.key != "hot_key")
# Process separately, then union
```

## 3. Caching

### When to Cache

```python
# GOOD: DataFrame used multiple times
df = spark.read.parquet("/data/")
df.cache()  # or df.persist()
df.count()  # Materialize cache

result1 = df.filter(df.age > 25).count()
result2 = df.groupBy("dept").avg("salary").collect()

df.unpersist()  # Release when done
```

### Storage Levels

```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)          # Default (deserialized in JVM heap)
df.persist(StorageLevel.MEMORY_AND_DISK)      # Spill to disk if no room
df.persist(StorageLevel.DISK_ONLY)            # Only on disk
df.persist(StorageLevel.MEMORY_ONLY_SER)      # Serialized (less memory, more CPU)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # Serialized + disk spill
df.persist(StorageLevel.OFF_HEAP)             # Off-heap memory
```

| Level | Memory | Disk | CPU | Notes |
|-------|--------|------|-----|-------|
| `MEMORY_ONLY` | High | No | Low | Default. Lost if evicted |
| `MEMORY_AND_DISK` | Medium | Fallback | Low | Recommended for production |
| `MEMORY_ONLY_SER` | Low | No | High | Good for large datasets |
| `DISK_ONLY` | None | Yes | High | When memory is scarce |

### Caching Anti-Patterns

```python
# BAD: Caching data used only once
df.cache()
df.write.parquet("/output/")  # Used once, cache wasted

# BAD: Caching too much data (causes eviction)
huge_df.cache()  # Evicts other useful caches

# BAD: Not materializing cache
df.cache()
df.show()  # Only caches what's needed for show()
df.count() # Must recompute!
# Fix: Call a full action after cache
df.cache()
df.count()  # Now everything is cached

# BAD: Forgetting to unpersist
df.cache()
# ... use df ...
df.unpersist()  # Always clean up
```

## 4. Memory Tuning

### Executor Memory

```python
# Total executor memory
spark.conf.set("spark.executor.memory", "8g")       # JVM heap
spark.conf.set("spark.executor.memoryOverhead", "2g") # Off-heap (Python, JVM overhead)

# Memory fraction
spark.conf.set("spark.memory.fraction", 0.6)         # 60% for execution + storage
spark.conf.set("spark.memory.storageFraction", 0.5)   # 50% of that for storage

# Example: 8g executor
# Unified: 8g * 0.6 = 4.8g
# Execution: ~2.4g (can borrow from storage)
# Storage: ~2.4g (can be evicted by execution)
# User memory: 8g * 0.4 = 3.2g
# Reserved: 300MB
```

### Sizing Executors

```python
# Rule of thumb for YARN:
# - 5 cores per executor (max parallelism without thrashing)
# - Leave 1 core per node for OS/YARN
# - Leave ~1GB per node for OS/YARN

# Example: 10 nodes, 16 cores, 64GB each
# Cores: (16 - 1) / 5 = 3 executors per node
# Memory: (64 - 1) / 3 = ~21GB per executor
# Overhead: max(384MB, 21 * 0.1) = 2.1GB
# spark.executor.memory = 21 - 2.1 ≈ 19g
# Total executors: 10 * 3 = 30 (minus 1 for driver = 29)

spark.conf.set("spark.executor.cores", "5")
spark.conf.set("spark.executor.memory", "19g")
spark.conf.set("spark.executor.memoryOverhead", "2g")
spark.conf.set("spark.executor.instances", "29")
```

### Garbage Collection

```python
# Use G1GC for large heaps (>4GB)
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")

# Reduce GC pressure:
# 1. Use serialized caching (MEMORY_ONLY_SER)
# 2. Prefer DataFrames over RDDs (Tungsten off-heap)
# 3. Avoid collecting large results to driver
```

## 5. Write Efficient Code

### Avoid UDFs When Possible

```python
# BAD: Python UDF (slow - data serialized to Python and back)
@F.udf(returnType=StringType())
def upper_udf(s):
    return s.upper() if s else None

df.withColumn("upper", upper_udf("name"))

# GOOD: Use built-in functions (runs in JVM, optimized)
df.withColumn("upper", F.upper("name"))

# BETTER UDF: Pandas UDF (vectorized, uses Arrow)
@F.pandas_udf(StringType())
def upper_pandas(s: pd.Series) -> pd.Series:
    return s.str.upper()

df.withColumn("upper", upper_pandas("name"))
```

**Performance hierarchy:**
1. Built-in functions (fastest - JVM optimized)
2. Spark SQL expressions
3. Pandas UDFs (vectorized via Arrow)
4. Python UDFs (slowest - serialization overhead)

### Predicate Pushdown

```python
# GOOD: Filter early (pushed to data source)
df = spark.read.parquet("/data/").filter(df.year == 2024)

# Catalyst automatically pushes predicates down
# But help it by filtering early in your code

# Check if pushdown is happening
df.explain()  # Look for "PushedFilters" in scan
```

### Column Pruning

```python
# GOOD: Select only needed columns early
df = spark.read.parquet("/data/").select("id", "name", "salary")

# BAD: Read all columns then filter
df = spark.read.parquet("/data/")  # Reads everything
result = df.select("id", "name")
```

### Avoid Collect on Large Data

```python
# BAD: Collecting large data to driver
all_data = df.collect()  # Can OOM the driver!

# GOOD: Use write to save large results
df.write.parquet("/output/")

# GOOD: Use take() for inspection
sample = df.take(10)
```

## Adaptive Query Execution (AQE)

AQE optimizes queries at runtime based on actual data statistics:

```python
spark.conf.set("spark.sql.adaptive.enabled", True)

# Feature 1: Coalesce shuffle partitions
# Automatically merges small post-shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")

# Feature 2: Convert sort-merge join to broadcast join
# If one side of join is smaller than expected after filtering
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", True)

# Feature 3: Skew join optimization
# Splits skewed partitions into smaller sub-partitions
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
```

## Monitoring and Debugging

### Spark UI

Key tabs to monitor:
- **Jobs**: Overall progress, failed/succeeded jobs
- **Stages**: Shuffle read/write, skew detection
- **Storage**: Cached DataFrames and their sizes
- **Environment**: All configuration values
- **SQL**: Query plans, execution details

### Detecting Problems

```python
# Check partition sizes (detect skew)
df.groupBy(F.spark_partition_id().alias("pid")).count().orderBy("count").show()

# Check query plan for issues
df.explain("formatted")

# Look for:
# - BroadcastHashJoin vs SortMergeJoin
# - Scans with PushedFilters
# - Unnecessary shuffles
```

## Key Configuration Reference

| Config | Default | Tuning Advice |
|--------|---------|---------------|
| `spark.sql.shuffle.partitions` | 200 | Increase for large data, decrease for small |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | Increase if you have memory |
| `spark.sql.adaptive.enabled` | true (3.2+) | Always enable |
| `spark.executor.memory` | 1g | Size based on workload |
| `spark.executor.cores` | 1 | 3-5 cores typical |
| `spark.memory.fraction` | 0.6 | Increase for cache-heavy workloads |
| `spark.serializer` | JavaSerializer | Use KryoSerializer for RDDs |
| `spark.default.parallelism` | Total cores | 2-3x total cores |
| `spark.sql.files.maxPartitionBytes` | 128MB | Controls input partition size |
| `spark.sql.files.openCostInBytes` | 4MB | Cost of opening a file |

## Key Exam Concepts

1. **Broadcast joins eliminate shuffle** for small + large table joins
2. **AQE handles skew, partition coalescing, and join optimization at runtime**
3. **`reduceByKey` > `groupByKey`** because it pre-aggregates before shuffle
4. **Built-in functions > Pandas UDFs > Python UDFs** (performance order)
5. **`coalesce` doesn't shuffle, `repartition` does**
6. **Cache data used multiple times, not once**
7. **Unified memory: execution and storage share memory pool**
8. **Predicate pushdown and column pruning happen automatically via Catalyst**
9. **Target ~128MB per partition, 2-4 partitions per core**
10. **Data skew: use salting, AQE, broadcast, or separate processing**
