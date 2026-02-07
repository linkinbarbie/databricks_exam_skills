# RDDs, DataFrames, and Datasets

## RDD (Resilient Distributed Dataset)

The foundational abstraction in Spark. A distributed, immutable collection of objects.

### Properties
1. **Resilient**: Fault-tolerant via lineage
2. **Distributed**: Partitioned across cluster
3. **Dataset**: Collection of records
4. **Immutable**: Transformations create new RDDs
5. **Lazy**: Computed only when an action is called

### Creating RDDs

```python
# From collection
rdd = sc.parallelize([1, 2, 3, 4, 5], numSlices=4)

# From file
rdd = sc.textFile("hdfs:///data/file.txt")
rdd = sc.textFile("s3://bucket/data/*.csv")

# From DataFrame
rdd = df.rdd

# From another RDD
rdd2 = rdd.map(lambda x: x * 2)
```

### Common RDD Operations

```python
# Transformations (lazy)
rdd.map(lambda x: x * 2)              # Apply function to each element
rdd.flatMap(lambda x: x.split(" "))   # Map + flatten
rdd.filter(lambda x: x > 10)          # Keep matching elements
rdd.distinct()                          # Remove duplicates
rdd.union(other_rdd)                   # Combine two RDDs
rdd.intersection(other_rdd)            # Common elements
rdd.subtract(other_rdd)                # Elements in rdd but not other

# Pair RDD transformations
pair_rdd = rdd.map(lambda x: (x[0], x))
pair_rdd.reduceByKey(lambda a, b: a + b)    # Reduce within each key
pair_rdd.groupByKey()                        # Group by key (avoid - expensive)
pair_rdd.sortByKey()                         # Sort by key
pair_rdd.mapValues(lambda v: v * 2)          # Map only values
pair_rdd.join(other_pair_rdd)                # Inner join
pair_rdd.leftOuterJoin(other_pair_rdd)       # Left join
pair_rdd.cogroup(other_pair_rdd)             # Group both RDDs by key

# Actions (eager)
rdd.collect()         # Return all elements
rdd.take(5)           # First 5 elements
rdd.first()           # First element
rdd.count()           # Count elements
rdd.reduce(lambda a, b: a + b)  # Aggregate all elements
rdd.foreach(print)    # Apply function to each element
rdd.saveAsTextFile("/output")
pair_rdd.countByKey()
pair_rdd.collectAsMap()
```

### reduceByKey vs groupByKey

```python
# PREFERRED: reduceByKey - combines at partition level first, then shuffles
rdd.reduceByKey(lambda a, b: a + b)
# Shuffle data: only aggregated values per key per partition

# AVOID: groupByKey - shuffles ALL data, then aggregates
rdd.groupByKey().mapValues(sum)
# Shuffle data: ALL values for each key
```

**Rule:** Always prefer `reduceByKey` over `groupByKey` when aggregating. `groupByKey` transfers all data across the network before reducing.

## DataFrame

A distributed collection of data organized into named columns. Equivalent to a table in a relational database.

### Key Advantages Over RDD
- **Catalyst Optimizer**: Automatic query optimization
- **Tungsten Engine**: Off-heap memory, code generation
- **Schema**: Named, typed columns
- **Interoperability**: Same API across Python, Scala, Java, R
- **Data source API**: Easy read/write from any format

### Creating DataFrames

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# From list of tuples
df = spark.createDataFrame(
    [(1, "Alice", 30), (2, "Bob", 25)],
    ["id", "name", "age"]
)

# With explicit schema
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True)
])
df = spark.createDataFrame(data, schema)

# From RDD
rdd = sc.parallelize([(1, "Alice"), (2, "Bob")])
df = rdd.toDF(["id", "name"])

# From files
df = spark.read.parquet("/data/users.parquet")
df = spark.read.csv("/data/users.csv", header=True, inferSchema=True)
df = spark.read.json("/data/events.json")
df = spark.read.format("delta").load("/data/delta_table/")

# From table
df = spark.table("database.table_name")
```

### DataFrame Operations

```python
# Selection
df.select("name", "age")
df.select(df.name, (df.age + 1).alias("next_age"))
df.selectExpr("name", "age + 1 as next_age")

# Filtering
df.filter(df.age > 25)
df.filter("age > 25")
df.where(df.name.like("A%"))
df.filter(df.name.isin("Alice", "Bob"))
df.filter(df.age.between(20, 30))

# Adding/Modifying columns
df.withColumn("senior", df.age > 60)
df.withColumn("name_upper", F.upper("name"))
df.withColumnRenamed("name", "full_name")

# Removing columns
df.drop("temp_col")
df.drop("col1", "col2")

# Sorting
df.orderBy("age")
df.orderBy(F.desc("age"))
df.sort(df.name.asc(), df.age.desc())

# Grouping
df.groupBy("dept").count()
df.groupBy("dept").agg(
    F.avg("salary").alias("avg_salary"),
    F.max("salary").alias("max_salary"),
    F.count("*").alias("total")
)

# Distinct / Dedup
df.distinct()
df.dropDuplicates(["email"])

# Union
df1.union(df2)           # By position (must have same number of columns)
df1.unionByName(df2)     # By column name (safer)
df1.unionByName(df2, allowMissingColumns=True)  # Handle missing columns

# Sampling
df.sample(fraction=0.1, seed=42)
```

### Schema Operations

```python
# View schema
df.printSchema()
df.schema              # Returns StructType
df.dtypes              # List of (name, type) tuples
df.columns             # List of column names

# Cast types
df.withColumn("age", df.age.cast("double"))
df.withColumn("date", F.to_date("date_str", "yyyy-MM-dd"))

# Enforce schema on read
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("scores", ArrayType(DoubleType())),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("state", StringType())
    ]))
])
df = spark.read.schema(schema).json("/data/")
```

### Complex Types

```python
# Array operations
df.select(F.explode("tags").alias("tag"))      # One row per element
df.select(F.posexplode("tags").alias("pos", "tag"))  # With position
df.filter(F.array_contains("tags", "python"))
df.select(F.size("tags").alias("num_tags"))
df.select(F.element_at("tags", 1))              # 1-based index

# Map operations
df.select(F.explode("properties"))               # key, value columns
df.select(F.map_keys("properties"))
df.select(F.map_values("properties"))
df.select(df.properties["key_name"])

# Struct operations
df.select("address.city")
df.select(F.col("address").getField("city"))

# JSON handling
df.select(F.from_json("json_col", schema).alias("parsed"))
df.select(F.to_json(F.struct("col1", "col2")).alias("json"))
df.select(F.get_json_object("json_col", "$.name"))
```

## Dataset (Scala/Java Only)

Datasets combine the type safety of RDDs with the optimization of DataFrames:

```scala
// Scala only
case class Person(id: Int, name: String, age: Int)

val ds: Dataset[Person] = spark.read.parquet("/data/").as[Person]

// Type-safe operations
ds.filter(_.age > 25)
ds.map(p => p.copy(name = p.name.toUpperCase))
ds.groupByKey(_.name).count()
```

**In PySpark, DataFrame IS the Dataset API.** There is no separate Dataset class.

## When to Use What

| Use Case | Best Choice | Why |
|----------|-------------|-----|
| ETL / SQL queries | **DataFrame** | Catalyst optimization, schema |
| Machine learning | **DataFrame** | MLlib uses DataFrames |
| Structured data | **DataFrame** | Schema, named columns |
| Unstructured data | **RDD** | No schema needed |
| Low-level control | **RDD** | Custom partitioning, fine-grained ops |
| Type safety (Scala) | **Dataset** | Compile-time checks |
| Python | **DataFrame** | No Dataset in PySpark |

## Converting Between Abstractions

```python
# DataFrame → RDD
rdd = df.rdd                    # RDD of Row objects

# RDD → DataFrame
df = rdd.toDF(["col1", "col2"])
df = spark.createDataFrame(rdd, schema)

# DataFrame → Pandas (small data only!)
pdf = df.toPandas()

# Pandas → DataFrame
df = spark.createDataFrame(pandas_df)
```

## Key Exam Concepts

1. **DataFrame > RDD for most use cases** (Catalyst optimization)
2. **RDDs have no Catalyst optimization** - manual tuning needed
3. **Dataset is Scala/Java only** - PySpark has no Dataset class
4. **`groupByKey` shuffles all data** - prefer `reduceByKey`
5. **DataFrames are lazy** just like RDDs
6. **Schema enforcement happens at read time** for DataFrames
7. **`union` matches by position**, `unionByName` matches by name
8. **`explode` creates one row per array/map element**
