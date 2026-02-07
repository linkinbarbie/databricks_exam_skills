# Transformations and Actions

## Lazy Evaluation

Spark uses **lazy evaluation**: transformations are not executed immediately. Instead, Spark builds a DAG (Directed Acyclic Graph) of transformations. Execution only happens when an **action** is called.

```python
# Nothing happens here (lazy)
df2 = df.filter(df.age > 25)        # Just records the transformation
df3 = df2.select("name", "salary")   # Adds to the DAG
df4 = df3.groupBy("name").sum()      # Adds to the DAG

# NOW execution happens (action)
df4.show()  # Triggers the entire DAG
```

**Benefits of lazy evaluation:**
- Catalyst optimizer can optimize the entire plan before execution
- Unnecessary computations are eliminated
- Transformations can be reordered for efficiency
- Predicate pushdown happens automatically

## Narrow vs Wide Transformations

### Narrow Transformations (No Shuffle)

Each input partition contributes to at most **one** output partition:

```
Partition 1 ──► Partition 1
Partition 2 ──► Partition 2
Partition 3 ──► Partition 3
```

| Operation | Description |
|-----------|-------------|
| `select()` | Select columns |
| `filter()` / `where()` | Filter rows |
| `withColumn()` | Add/modify column |
| `drop()` | Remove column |
| `map()` | Apply function to each element (RDD) |
| `flatMap()` | Map + flatten (RDD) |
| `mapPartitions()` | Apply function to each partition |
| `union()` | Combine two DataFrames/RDDs |
| `coalesce()` | Reduce partitions (without shuffle) |
| `sample()` | Random sampling |

**Narrow transforms are fast** because data stays on the same node.

### Wide Transformations (Cause Shuffle)

Each input partition may contribute to **multiple** output partitions:

```
Partition 1 ──┬──► Partition A
              ├──► Partition B
Partition 2 ──┤
              ├──► Partition A
Partition 3 ──┴──► Partition B
```

| Operation | Description |
|-----------|-------------|
| `groupBy()` | Group by columns |
| `join()` | Join two DataFrames |
| `orderBy()` / `sort()` | Sort data |
| `repartition()` | Redistribute data |
| `distinct()` | Remove duplicates |
| `reduceByKey()` | Aggregate by key (RDD) |
| `groupByKey()` | Group by key (RDD) |
| `aggregateByKey()` | Aggregate by key with combiner (RDD) |
| `sortByKey()` | Sort by key (RDD) |
| `cogroup()` | Group multiple RDDs by key |
| `pivot()` | Pivot table |

**Wide transforms are expensive** because they require data to move across the network (shuffle).

### Stage Boundaries

Spark creates a new stage at each wide transformation:

```python
# Stage 0 (narrow transforms - pipelined)
result = (df
    .filter(df.age > 25)        # narrow
    .select("name", "salary")    # narrow
    .withColumn("bonus", df.salary * 0.1)  # narrow

    # ── SHUFFLE BOUNDARY (Stage 1) ──
    .groupBy("name")             # wide
    .agg(F.sum("salary"))

    # ── SHUFFLE BOUNDARY (Stage 2) ──
    .orderBy("sum(salary)")      # wide
    .collect()                    # ACTION
)
```

## Complete Transformation Reference (DataFrame)

### Column Operations
```python
# Select
df.select("col1", "col2")
df.select(F.col("col1"), F.col("col2"))
df.selectExpr("col1", "col2 * 2 as doubled")

# Add/Modify
df.withColumn("new_col", F.lit(0))
df.withColumn("upper_name", F.upper("name"))

# Rename
df.withColumnRenamed("old_name", "new_name")
df.toDF("col1", "col2", "col3")  # Rename all columns

# Drop
df.drop("col1")
df.drop("col1", "col2")

# Cast
df.withColumn("age", F.col("age").cast("double"))
```

### Row Operations
```python
# Filter
df.filter(F.col("age") > 25)
df.filter((F.col("age") > 25) & (F.col("dept") == "IT"))
df.filter(F.col("name").like("A%"))
df.filter(F.col("id").isin([1, 2, 3]))
df.filter(F.col("value").between(10, 100))
df.filter(F.col("email").isNotNull())
df.where("age > 25 AND dept = 'IT'")  # SQL expression

# Distinct
df.distinct()
df.dropDuplicates()
df.dropDuplicates(["email"])  # Dedup by subset of columns

# Sort
df.orderBy("age")
df.orderBy(F.asc("name"), F.desc("age"))
df.sort("age")
df.sortWithinPartitions("age")  # Sort within each partition (no shuffle)

# Limit
df.limit(100)

# Sample
df.sample(fraction=0.1, seed=42)
df.sample(withReplacement=True, fraction=0.5)
```

### Aggregation Operations
```python
# Group By + Aggregate
df.groupBy("dept").count()
df.groupBy("dept", "role").agg(
    F.count("*").alias("total"),
    F.avg("salary").alias("avg_salary"),
    F.sum("salary").alias("total_salary"),
    F.min("salary").alias("min_salary"),
    F.max("salary").alias("max_salary"),
    F.stddev("salary").alias("std_salary"),
    F.collect_list("name").alias("names"),
    F.collect_set("name").alias("unique_names"),
    F.countDistinct("name").alias("unique_count"),
    F.first("name").alias("first_name"),
    F.last("name").alias("last_name"),
    F.approx_count_distinct("name").alias("approx_unique")
)

# Pivot
df.groupBy("year").pivot("quarter", ["Q1", "Q2", "Q3", "Q4"]).sum("revenue")

# Rollup (hierarchical aggregation)
df.rollup("country", "city").sum("revenue")

# Cube (all combinations)
df.cube("country", "city").sum("revenue")
```

### Join Operations
```python
# Types: inner, left, right, outer, left_semi, left_anti, cross
df1.join(df2, "id")                              # inner on shared column
df1.join(df2, df1.id == df2.user_id)             # inner on expression
df1.join(df2, "id", "left")                      # left outer
df1.join(df2, ["id", "date"])                     # multi-column join
df1.join(df2, (df1.id == df2.id) & (df1.dt == df2.dt))  # complex condition
df1.crossJoin(df2)                                # cartesian product
df1.join(broadcast(df2), "id")                    # broadcast hint
```

### Set Operations
```python
df1.union(df2)                                    # Combine (by position)
df1.unionByName(df2)                              # Combine (by name)
df1.unionByName(df2, allowMissingColumns=True)    # Handle different schemas
df1.intersect(df2)                                # Common rows
df1.exceptAll(df2)                                # Rows in df1 not in df2
```

### Partition Operations
```python
df.repartition(200)                    # Hash repartition (shuffle)
df.repartition("country")             # Partition by column (shuffle)
df.repartition(200, "country")        # Both
df.coalesce(10)                        # Reduce partitions (no shuffle)
df.rdd.getNumPartitions()             # Check partition count
```

## Complete Actions Reference (DataFrame)

| Action | Returns | Description |
|--------|---------|-------------|
| `show(n)` | None | Display first n rows |
| `collect()` | List[Row] | All rows to driver |
| `take(n)` | List[Row] | First n rows to driver |
| `first()` | Row | First row |
| `head(n)` | List[Row] | First n rows |
| `count()` | int | Number of rows |
| `describe()` | DataFrame | Summary statistics |
| `toPandas()` | pandas.DataFrame | Convert to Pandas |
| `foreach(f)` | None | Apply function per row |
| `foreachPartition(f)` | None | Apply function per partition |
| `write.parquet()` | None | Write to Parquet |
| `write.csv()` | None | Write to CSV |
| `write.json()` | None | Write to JSON |
| `write.saveAsTable()` | None | Write as managed table |

## Write Operations

```python
# Write modes
df.write.mode("overwrite").parquet("/output/")   # Replace existing
df.write.mode("append").parquet("/output/")      # Add to existing
df.write.mode("ignore").parquet("/output/")      # Skip if exists
df.write.mode("error").parquet("/output/")       # Fail if exists (default)

# Partitioned write
df.write.partitionBy("year", "month").parquet("/output/")

# Bucketed write (for optimized joins)
df.write.bucketBy(32, "user_id").sortBy("user_id").saveAsTable("users")

# Number of output files
df.coalesce(1).write.parquet("/output/")      # Single file
df.repartition(10).write.parquet("/output/")   # 10 files
```

## Key Exam Concepts

1. **Transformations are lazy, actions trigger execution**
2. **Narrow transforms: no shuffle, data stays local**
3. **Wide transforms: shuffle, data moves across network**
4. **Stages are bounded by shuffles**
5. **Within a stage, narrow transforms are pipelined**
6. **`coalesce` reduces partitions without shuffle, `repartition` always shuffles**
7. **`sortWithinPartitions` avoids full shuffle (sorts locally)**
8. **Write mode "error" is the default**
9. **`union` matches by position, `unionByName` matches by column name**
10. **`collect()` brings ALL data to driver - be careful with large data!**
