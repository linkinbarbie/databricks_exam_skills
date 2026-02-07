# Delta Live Tables (DLT)

## What is DLT?

Delta Live Tables is a **declarative framework** for building reliable data pipelines. You define *what* you want, not *how* to build it.

```
Traditional ETL:          vs          DLT:
─────────────────                    ─────
1. Read source                       Define tables
2. Transform                         Define expectations
3. Handle errors                     DLT handles the rest
4. Write to target
5. Manage dependencies
6. Handle failures
7. Track lineage
```

## Core Concepts

### 1. Pipeline

A pipeline is a collection of datasets (tables/views) and their dependencies.

```
┌─────────────────────────────────────────────────────────────────┐
│                         DLT PIPELINE                             │
│                                                                  │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│   │  SOURCE  │───▶│  BRONZE  │───▶│  SILVER  │───▶│   GOLD   │  │
│   │ (stream) │    │ (raw)    │    │ (clean)  │    │ (agg)    │  │
│   └──────────┘    └──────────┘    └──────────┘    └──────────┘  │
│                                                                  │
│   DLT automatically:                                             │
│   • Manages dependencies                                         │
│   • Handles failures/retries                                     │
│   • Tracks data quality                                          │
│   • Provides lineage                                             │
└─────────────────────────────────────────────────────────────────┘
```

### 2. Datasets

| Type | Keyword | Description | Use Case |
|------|---------|-------------|----------|
| **Streaming Table** | `STREAMING TABLE` | Append-only, incremental | Raw ingestion, event data |
| **Materialized View** | `MATERIALIZED VIEW` | Recomputed on each run | Aggregations, snapshots |
| **View** | `VIEW` | Not materialized | Intermediate transforms |

## Python Syntax

### Streaming Table (Incremental)

```python
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="bronze_orders",
    comment="Raw orders from source"
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/data/orders/")
    )
```

### Materialized View (Batch)

```python
@dlt.table(
    name="silver_orders",
    comment="Cleaned orders"
)
def silver_orders():
    return (
        dlt.read("bronze_orders")
        .filter(col("amount") > 0)
        .dropDuplicates(["order_id"])
        .withColumn("processed_at", current_timestamp())
    )
```

### View (Not Persisted)

```python
@dlt.view(
    name="orders_with_customers"
)
def orders_with_customers():
    orders = dlt.read("silver_orders")
    customers = dlt.read("silver_customers")
    return orders.join(customers, "customer_id")
```

### Gold Aggregation

```python
@dlt.table(
    name="gold_daily_sales",
    comment="Daily sales aggregation"
)
def gold_daily_sales():
    return (
        dlt.read("silver_orders")
        .groupBy("order_date")
        .agg(
            sum("amount").alias("total_sales"),
            count("order_id").alias("order_count")
        )
    )
```

## SQL Syntax

### Streaming Table

```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT "Raw orders from source"
AS SELECT * FROM cloud_files("/data/orders/", "json")
```

### Materialized View

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_orders
COMMENT "Cleaned orders"
AS
SELECT
    order_id,
    customer_id,
    amount,
    order_date,
    current_timestamp() AS processed_at
FROM LIVE.bronze_orders
WHERE amount > 0
```

### Live Table Reference

```sql
-- Use LIVE.table_name to reference other DLT tables
CREATE OR REFRESH MATERIALIZED VIEW gold_summary
AS
SELECT
    order_date,
    SUM(amount) as total_sales
FROM LIVE.silver_orders
GROUP BY order_date
```

## Data Quality: Expectations

### Define Expectations

```python
@dlt.table(
    name="silver_orders"
)
@dlt.expect("valid_amount", "amount > 0")
@dlt.expect("valid_customer", "customer_id IS NOT NULL")
def silver_orders():
    return dlt.read("bronze_orders")
```

### Expectation Actions

| Decorator | Action on Failure |
|-----------|-------------------|
| `@dlt.expect("name", "condition")` | Warn, keep row |
| `@dlt.expect_or_drop("name", "condition")` | Drop failing rows |
| `@dlt.expect_or_fail("name", "condition")` | Fail pipeline |

### Multiple Expectations

```python
@dlt.table(name="silver_orders")
@dlt.expect_all({
    "valid_amount": "amount > 0",
    "valid_customer": "customer_id IS NOT NULL",
    "valid_date": "order_date <= current_date()"
})
def silver_orders():
    return dlt.read("bronze_orders")

# Or drop all failures
@dlt.table(name="silver_orders_strict")
@dlt.expect_all_or_drop({
    "valid_amount": "amount > 0",
    "valid_customer": "customer_id IS NOT NULL"
})
def silver_orders_strict():
    return dlt.read("bronze_orders")
```

### SQL Expectations

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_orders (
    CONSTRAINT valid_amount EXPECT (amount > 0),
    CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT * FROM LIVE.bronze_orders
```

## Streaming vs Batch

### Streaming Table (Append-Only)

```python
# Reads incrementally, appends new data
@dlt.table(name="events")
def events():
    return spark.readStream.format("kafka").load()

# Use for:
# - Event data
# - Logs
# - Any append-only source
```

### Materialized View (Full Refresh)

```python
# Recomputes entire table each run
@dlt.table(name="daily_summary")
def daily_summary():
    return dlt.read("events").groupBy("date").count()

# Use for:
# - Aggregations
# - Slowly changing dimensions
# - Any data that needs full recompute
```

## Change Data Capture (CDC)

### Apply Changes

```python
dlt.create_streaming_table("target_customers")

dlt.apply_changes(
    target="target_customers",
    source="cdc_source",
    keys=["customer_id"],
    sequence_by="timestamp",
    apply_as_deletes=expr("operation = 'DELETE'"),
    apply_as_truncates=expr("operation = 'TRUNCATE'"),
    column_list=["customer_id", "name", "email", "updated_at"]
)
```

### SQL CDC

```sql
CREATE OR REFRESH STREAMING TABLE target_customers;

APPLY CHANGES INTO LIVE.target_customers
FROM STREAM(LIVE.cdc_source)
KEYS (customer_id)
SEQUENCE BY timestamp
COLUMNS * EXCEPT (operation)
STORED AS SCD TYPE 1
```

### SCD Types

| Type | Behavior |
|------|----------|
| `SCD TYPE 1` | Overwrite (default) |
| `SCD TYPE 2` | Keep history with `__START_AT`, `__END_AT` |

## Pipeline Configuration

### Development vs Production

```json
{
    "development": true,       // Dev mode: no retries, relaxed scheduling
    "continuous": false,       // Triggered vs continuous
    "channel": "PREVIEW",      // PREVIEW or CURRENT runtime
    "photon": true,           // Use Photon acceleration
    "clusters": [
        {
            "label": "default",
            "num_workers": 4
        }
    ]
}
```

### Pipeline Settings

| Setting | Description |
|---------|-------------|
| `development` | True for dev (no retries), False for prod |
| `continuous` | True = always running, False = triggered |
| `target` | Unity Catalog schema for output tables |
| `catalog` | Unity Catalog catalog name |

## Best Practices

### 1. Medallion Architecture

```python
# Bronze: Raw data
@dlt.table(name="bronze_events")
def bronze():
    return spark.readStream.format("cloudFiles").load("/raw/")

# Silver: Cleaned
@dlt.table(name="silver_events")
@dlt.expect_or_drop("valid", "event_id IS NOT NULL")
def silver():
    return dlt.read_stream("bronze_events").dropDuplicates()

# Gold: Aggregated
@dlt.table(name="gold_metrics")
def gold():
    return dlt.read("silver_events").groupBy("date").count()
```

### 2. Use Expectations Everywhere

```python
# Catch data quality issues early
@dlt.expect("not_null_id", "id IS NOT NULL")
@dlt.expect("valid_amount", "amount >= 0")
@dlt.expect("recent_data", "event_date >= '2024-01-01'")
```

### 3. Parameterize with Widgets

```python
# Access pipeline parameters
source_path = spark.conf.get("source_path", "/default/path")

@dlt.table
def bronze():
    return spark.readStream.load(source_path)
```

### 4. Modularize Code

```python
# utils.py
def clean_data(df):
    return df.dropDuplicates().filter("amount > 0")

# pipeline.py
from utils import clean_data

@dlt.table
def silver():
    return clean_data(dlt.read("bronze"))
```

## Monitoring & Debugging

### Event Log

```sql
-- Query DLT event log
SELECT * FROM event_log(TABLE(my_pipeline))
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
```

### Data Quality Metrics

```sql
-- Check expectation results
SELECT
    details:flow_name,
    details:expectation:name,
    details:expectation:passed_records,
    details:expectation:failed_records
FROM event_log(TABLE(my_pipeline))
WHERE event_type = 'flow_progress'
```

### Lineage

DLT automatically tracks:
- Table dependencies
- Column-level lineage
- Data flow visualization

View in: **Pipeline UI → Lineage tab**

## Exam Questions

### Q1: Streaming vs Materialized
**When should you use a Streaming Table vs Materialized View?**

- **Streaming Table**: Append-only data, incremental processing
- **Materialized View**: Aggregations, full recompute needed

### Q2: Expectations
**What does `@dlt.expect_or_drop` do?**

Drops rows that fail the expectation (doesn't fail the pipeline)

### Q3: LIVE Keyword
**What does `LIVE.table_name` mean in DLT SQL?**

References another table in the same DLT pipeline

### Q4: CDC
**What does `apply_changes` do?**

Processes CDC data (inserts, updates, deletes) into a target table

### Q5: Development Mode
**What's the difference between development and production mode?**

- Development: No retries, faster iteration, relaxed scheduling
- Production: Retries on failure, scheduled runs, production-grade

## Key Commands

```sql
-- Create streaming table
CREATE OR REFRESH STREAMING TABLE table_name AS ...

-- Create materialized view
CREATE OR REFRESH MATERIALIZED VIEW view_name AS ...

-- Reference DLT table
SELECT * FROM LIVE.other_table

-- Add expectation
CONSTRAINT name EXPECT (condition) ON VIOLATION DROP ROW

-- Apply CDC changes
APPLY CHANGES INTO LIVE.target FROM STREAM(LIVE.source) ...
```
