# Spark SQL & Catalyst Optimizer

## Catalyst Optimizer Overview

Catalyst is Spark SQL's query optimizer. It transforms logical plans into optimized physical execution plans.

### Optimization Pipeline

```
SQL Query / DataFrame API
        │
        ▼
┌─────────────────┐
│ 1. PARSING      │  Convert SQL string → Unresolved Logical Plan
│                 │  (DataFrame API skips this step)
└────────┬────────┘
         ▼
┌─────────────────┐
│ 2. ANALYSIS     │  Resolve column names, types, functions
│                 │  Uses Catalog to resolve references
└────────┬────────┘
         ▼
┌─────────────────┐
│ 3. OPTIMIZATION │  Apply rule-based + cost-based optimizations
│   (Logical)     │  Predicate pushdown, constant folding, etc.
└────────┬────────┘
         ▼
┌─────────────────┐
│ 4. PHYSICAL     │  Generate physical plan candidates
│   PLANNING      │  Choose best plan via cost model
└────────┬────────┘
         ▼
┌─────────────────┐
│ 5. CODE GEN     │  Whole-Stage Code Generation (Tungsten)
│                 │  Compile to JVM bytecode
└────────┬────────┘
         ▼
    Execution
```

### Viewing Query Plans

```python
# Simple physical plan
df.explain()

# Full plan (parsed → analyzed → optimized → physical)
df.explain(True)

# Formatted physical plan (easier to read)
df.explain("formatted")

# Extended (includes all plan types)
df.explain("extended")

# Cost-based (includes statistics)
df.explain("cost")
```

**Example output:**
```
== Physical Plan ==
*(2) HashAggregate(keys=[dept], functions=[sum(salary), count(1)])
+- Exchange hashpartitioning(dept, 200)          ← SHUFFLE
   +- *(1) HashAggregate(keys=[dept], functions=[partial_sum(salary), partial_count(1)])
      +- *(1) Filter (age > 25)                  ← PREDICATE
         +- *(1) FileScan parquet [dept,salary,age]  ← SCAN
               PushedFilters: [GreaterThan(age,25)]  ← PUSHED TO SOURCE
```

## Key Catalyst Optimizations

### 1. Predicate Pushdown

Moves filters as close to the data source as possible:

```python
# You write:
df = spark.read.parquet("/data/")
result = df.join(other, "id").filter(df.year == 2024)

# Catalyst rewrites to:
# Filter pushed BEFORE the join (reads less data)
df = spark.read.parquet("/data/").filter(df.year == 2024)  # Pushed to scan
result = df.join(other, "id")
```

**Works with:** Parquet, ORC, JDBC, Delta Lake
**Pushed predicates:** =, !=, <, >, <=, >=, IN, IS NULL, IS NOT NULL, AND, OR

### 2. Column Pruning

Only reads columns that are actually used:

```python
# You write:
df = spark.read.parquet("/data/")  # 100 columns in file
result = df.select("name", "salary")

# Catalyst reads only 2 columns from Parquet (columnar format)
```

### 3. Constant Folding

Evaluates constant expressions at compile time:

```python
# You write:
df.filter(F.col("timestamp") > F.lit("2024-01-01").cast("timestamp"))

# Catalyst pre-computes the constant, doesn't evaluate per-row
```

### 4. Boolean Expression Simplification

```python
# Simplifies boolean logic
# WHERE true AND x > 5  →  WHERE x > 5
# WHERE false OR x > 5  →  WHERE x > 5
# WHERE NOT (NOT x > 5) →  WHERE x > 5
```

### 5. Join Reordering

Catalyst reorders joins to minimize intermediate data:

```python
# You write:
a.join(b, "id").join(c, "id")

# Catalyst may reorder to:
a.join(c, "id").join(b, "id")
# If c is smaller → smaller intermediate result
```

### 6. Combine Filters

```python
# You write:
df.filter(df.age > 20).filter(df.age < 50)

# Catalyst combines:
df.filter((df.age > 20) & (df.age < 50))
```

### 7. Null Propagation

```python
# WHERE null = null  →  eliminates (null != null in SQL)
# COALESCE(non_nullable_col, default)  →  non_nullable_col
```

## Tungsten Engine

Tungsten is Spark's execution engine focused on CPU and memory efficiency:

### Key Features

1. **Off-Heap Memory Management**
   - Manages memory directly (not JVM garbage collected)
   - Binary format (UnsafeRow) instead of Java objects
   - Reduces GC pressure

2. **Whole-Stage Code Generation**
   - Compiles query plan into a single Java function
   - Eliminates virtual function calls
   - CPU cache-friendly

3. **Cache-Aware Computation**
   - Optimizes for CPU L1/L2 cache
   - Sequential memory access patterns

### UnsafeRow Format

```
Traditional Java Object:
┌──────────────────────────────────────┐
│ Object Header (16 bytes)             │
│ String name → [pointer to heap]      │
│ int age                              │
│ double salary → [pointer to heap]    │
└──────────────────────────────────────┘
  Multiple objects, pointers, GC pressure

Tungsten UnsafeRow (binary format):
┌──────────────────────────────────────────┐
│ Null bitmap │ Fixed-length │ Variable    │
│   (1 bit    │   values    │  length     │
│  per field) │ (offset +   │  values     │
│             │  length)    │ (contiguous)│
└──────────────────────────────────────────┘
  Single contiguous memory block, no GC
```

### Whole-Stage Code Generation

```python
# Check if whole-stage codegen is active
df.explain()
# Look for *(n) prefix in plan:
# *(1) Filter (age > 25)     ← * means codegen active
# *(1) FileScan parquet       ← Same stage, pipelined

# Enable/disable (enabled by default)
spark.conf.set("spark.sql.codegen.wholeStage", True)
```

## Spark SQL Features

### Temporary Views

```python
# Session-scoped view (visible only in current SparkSession)
df.createOrReplaceTempView("employees")
spark.sql("SELECT * FROM employees WHERE age > 25")

# Global view (visible across all SparkSessions in same app)
df.createOrReplaceGlobalTempView("employees")
spark.sql("SELECT * FROM global_temp.employees")
```

### SQL Functions

```sql
-- String
UPPER(name), LOWER(name), TRIM(name), LENGTH(name)
CONCAT(first, ' ', last), SUBSTRING(name, 1, 3)
REGEXP_REPLACE(text, '\\d+', 'NUM')
SPLIT(csv, ','), CONCAT_WS(',', col1, col2)

-- Date/Time
CURRENT_DATE(), CURRENT_TIMESTAMP()
DATE_ADD(date, 7), DATE_SUB(date, 7)
DATEDIFF(end, start), MONTHS_BETWEEN(end, start)
YEAR(date), MONTH(date), DAY(date)
DATE_FORMAT(date, 'yyyy-MM-dd')
TO_DATE(string, 'MM/dd/yyyy')
TO_TIMESTAMP(string, 'yyyy-MM-dd HH:mm:ss')

-- Aggregate
COUNT(*), COUNT(DISTINCT col), SUM(col), AVG(col)
MIN(col), MAX(col), STDDEV(col), VARIANCE(col)
COLLECT_LIST(col), COLLECT_SET(col)
FIRST(col), LAST(col)
PERCENTILE_APPROX(col, 0.5)

-- Conditional
CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END
IF(condition, true_val, false_val)
COALESCE(col1, col2, 'default')
NULLIF(col1, col2)
NVL(col, default), NVL2(col, not_null_val, null_val)

-- Type casting
CAST(col AS INT), CAST(col AS STRING), CAST(col AS DATE)

-- Higher-order functions (arrays)
TRANSFORM(array, x -> x + 1)
FILTER(array, x -> x > 0)
EXISTS(array, x -> x > 100)
AGGREGATE(array, 0, (acc, x) -> acc + x)
```

### Window Functions in SQL

```sql
-- Ranking
ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)
RANK() OVER (PARTITION BY dept ORDER BY salary DESC)
DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC)
NTILE(4) OVER (PARTITION BY dept ORDER BY salary)
PERCENT_RANK() OVER (PARTITION BY dept ORDER BY salary)

-- Analytic
LAG(salary, 1, 0) OVER (PARTITION BY dept ORDER BY date)
LEAD(salary, 1, 0) OVER (PARTITION BY dept ORDER BY date)
FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY date)
LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

-- Aggregate windows
SUM(salary) OVER (PARTITION BY dept ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
AVG(salary) OVER (PARTITION BY dept
    ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS moving_avg
COUNT(*) OVER (PARTITION BY dept) AS dept_total
```

### Window Frame Specifications

```sql
-- ROWS frame (physical position)
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW     -- All rows up to current
ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING              -- Sliding window of 7
ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING       -- Current to end

-- RANGE frame (logical value range)
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW     -- Default for ORDER BY
RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
```

**Key difference:**
- `ROWS`: Based on physical row position
- `RANGE`: Based on value of ORDER BY column

### Common Table Expressions (CTEs)

```sql
WITH
    active_users AS (
        SELECT * FROM users WHERE status = 'active'
    ),
    user_orders AS (
        SELECT user_id, COUNT(*) AS order_count
        FROM orders GROUP BY user_id
    )
SELECT a.name, COALESCE(o.order_count, 0) AS orders
FROM active_users a
LEFT JOIN user_orders o ON a.id = o.user_id;
```

### Subqueries

```sql
-- Scalar subquery
SELECT *, (SELECT AVG(salary) FROM employees) AS avg_sal
FROM employees;

-- IN subquery
SELECT * FROM employees
WHERE dept_id IN (SELECT id FROM departments WHERE active = true);

-- EXISTS subquery
SELECT * FROM employees e
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id);

-- Correlated subquery
SELECT e.*, (
    SELECT COUNT(*) FROM orders o WHERE o.emp_id = e.id
) AS order_count
FROM employees e;
```

## Cost-Based Optimizer (CBO)

CBO uses table and column statistics to make decisions:

```sql
-- Collect table statistics
ANALYZE TABLE orders COMPUTE STATISTICS;

-- Collect column statistics
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id, amount;

-- View statistics
DESCRIBE EXTENDED orders;
```

**CBO uses statistics for:**
- Join strategy selection (broadcast vs sort-merge)
- Join reordering
- Aggregate strategy selection

```python
# Enable CBO (usually enabled by default)
spark.conf.set("spark.sql.cbo.enabled", True)
spark.conf.set("spark.sql.cbo.joinReorder.enabled", True)
```

## Key Exam Concepts

1. **Catalyst optimizes both SQL and DataFrame API identically**
2. **Predicate pushdown**: filters pushed to data source (Parquet, JDBC, etc.)
3. **Column pruning**: only needed columns read from columnar formats
4. **Whole-stage codegen**: compiles entire stages to single JVM function
5. **Tungsten UnsafeRow**: binary format, off-heap, no GC pressure
6. **`explain()` shows the physical plan** - look for PushedFilters, codegen
7. **CBO requires `ANALYZE TABLE`** to collect statistics
8. **Window functions**: `ROWS` = physical position, `RANGE` = logical value
9. **Global temp views**: accessed via `global_temp.view_name`
10. **CTEs improve readability** but are not materialized (re-evaluated each reference)
