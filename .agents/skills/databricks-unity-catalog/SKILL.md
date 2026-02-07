---
name: databricks-unity-catalog
description: Master Unity Catalog, Delta Lake, and Databricks for certification exams - covers data governance, permissions, SQL, and platform architecture
version: 1.0.0
author: Custom
tags: [Databricks, Unity Catalog, Delta Lake, Certification, Data Engineering, Governance]
dependencies: []
---

# Databricks & Unity Catalog Mastery

## When to Use This Skill

Use this skill when you need to:
- **Prepare for Databricks certification exams**
- **Understand Unity Catalog** hierarchy and governance
- **Write correct SQL** for Delta Lake and UC
- **Explain data governance concepts** (permissions, lineage, audit)
- **Debug permission issues** in Unity Catalog

## Unity Catalog Hierarchy

```
┌─────────────────────────────────────────────────────────────────┐
│                         METASTORE                                │
│         (Top-level container, one per region)                    │
│                              │                                   │
│         ┌────────────────────┼────────────────────┐              │
│         ▼                    ▼                    ▼              │
│   ┌──────────┐        ┌──────────┐        ┌──────────┐          │
│   │ CATALOG  │        │ CATALOG  │        │ CATALOG  │          │
│   │ (prod)   │        │ (dev)    │        │ (sandbox)│          │
│   └────┬─────┘        └──────────┘        └──────────┘          │
│        │                                                         │
│   ┌────┴────────────────────┐                                   │
│   ▼                         ▼                                   │
│ ┌──────────┐          ┌──────────┐                              │
│ │ SCHEMA   │          │ SCHEMA   │                              │
│ │ (sales)  │          │ (finance)│                              │
│ └────┬─────┘          └──────────┘                              │
│      │                                                          │
│ ┌────┴─────────────────────────────┐                            │
│ ▼              ▼            ▼      ▼                            │
│ TABLE       VIEW      FUNCTION  VOLUME                          │
│ (orders)  (summary)  (udf)     (files)                          │
└─────────────────────────────────────────────────────────────────┘
```

### Three-Level Namespace
```sql
-- catalog.schema.object
SELECT * FROM prod.sales.orders;
SELECT * FROM dev.finance.transactions;
```

## Key Concepts for Certification

### 1. Securable Objects

| Object | Description | Example |
|--------|-------------|---------|
| **Metastore** | Top-level container (1 per region) | `aws-us-east-1-metastore` |
| **Catalog** | First level of namespace | `prod`, `dev`, `sandbox` |
| **Schema** | Second level (aka database) | `sales`, `finance`, `hr` |
| **Table** | Managed or External table | `orders`, `customers` |
| **View** | Virtual table from query | `daily_summary` |
| **Volume** | File storage (new in UC) | `/Volumes/prod/raw/files/` |
| **Function** | UDF | `calculate_tax()` |
| **Model** | ML model in registry | `fraud_detector` |

### 2. Managed vs External Tables

```sql
-- MANAGED TABLE (Unity Catalog controls storage)
CREATE TABLE prod.sales.orders (
    order_id INT,
    amount DECIMAL(10,2)
);
-- Data stored in: metastore_root/catalog/schema/table/

-- EXTERNAL TABLE (you control storage)
CREATE TABLE prod.sales.external_orders (
    order_id INT,
    amount DECIMAL(10,2)
)
LOCATION 's3://my-bucket/orders/';
-- Data stays at your S3 location
```

**Key Difference:**
- **Managed**: DROP TABLE deletes data
- **External**: DROP TABLE keeps data, only removes metadata

### 3. Permissions Model

```sql
-- Grant catalog access
GRANT USE CATALOG ON CATALOG prod TO `data-team`;

-- Grant schema access
GRANT USE SCHEMA ON SCHEMA prod.sales TO `data-team`;

-- Grant table access
GRANT SELECT ON TABLE prod.sales.orders TO `data-team`;

-- Grant all privileges
GRANT ALL PRIVILEGES ON SCHEMA prod.sales TO `data-admins`;

-- Revoke access
REVOKE SELECT ON TABLE prod.sales.orders FROM `data-team`;
```

**Permission Hierarchy:**
```
USE CATALOG → USE SCHEMA → SELECT/MODIFY on tables
```
You need parent permissions to access children.

### 4. Row-Level Security

```sql
-- Create a function for row filter
CREATE FUNCTION prod.sales.row_filter(region STRING)
RETURN IF(IS_MEMBER('global-team'), true, region = current_user_region());

-- Apply to table
ALTER TABLE prod.sales.orders
SET ROW FILTER prod.sales.row_filter ON (region);
```

### 5. Column Masking

```sql
-- Create masking function
CREATE FUNCTION prod.sales.mask_pii(value STRING)
RETURN IF(IS_MEMBER('pii-access'), value, 'XXX-MASKED');

-- Apply to column
ALTER TABLE prod.sales.customers
ALTER COLUMN ssn SET MASK prod.sales.mask_pii;
```

### 6. Data Lineage

Unity Catalog automatically tracks:
- **Table lineage**: Which tables feed into other tables
- **Column lineage**: Which columns derive from which sources
- **Query history**: Who queried what, when

View in UI: **Data Explorer → Table → Lineage tab**

## Delta Lake Essentials

### ACID Transactions
```sql
-- All operations are ACID compliant
INSERT INTO orders VALUES (1, 100.00);
UPDATE orders SET amount = 110.00 WHERE order_id = 1;
DELETE FROM orders WHERE order_id = 1;
MERGE INTO orders USING updates ON orders.id = updates.id
  WHEN MATCHED THEN UPDATE SET amount = updates.amount
  WHEN NOT MATCHED THEN INSERT *;
```

### Time Travel
```sql
-- Query historical version
SELECT * FROM orders VERSION AS OF 5;
SELECT * FROM orders TIMESTAMP AS OF '2024-01-01';

-- Restore to previous version
RESTORE TABLE orders TO VERSION AS OF 5;
```

### Table Maintenance
```sql
-- Optimize file layout (compaction)
OPTIMIZE orders;

-- Z-Order for query performance
OPTIMIZE orders ZORDER BY (customer_id, order_date);

-- Remove old files (vacuum)
VACUUM orders RETAIN 168 HOURS;  -- 7 days minimum

-- Analyze table statistics
ANALYZE TABLE orders COMPUTE STATISTICS;
```

### Schema Evolution
```sql
-- Add column
ALTER TABLE orders ADD COLUMN discount DECIMAL(5,2);

-- Enable automatic schema evolution
SET spark.databricks.delta.schema.autoMerge.enabled = true;
```

## SQL Warehouse vs Clusters

| Feature | SQL Warehouse | All-Purpose Cluster |
|---------|---------------|---------------------|
| Use case | BI, SQL queries | Notebooks, ML, ETL |
| Scaling | Auto-scales | Manual or autoscale |
| Cost | Per-query (serverless) | Per-hour |
| Languages | SQL only | Python, Scala, R, SQL |
| Best for | Dashboards, ad-hoc | Development, training |

## Common Exam Topics

### 1. Unity Catalog Setup
```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS prod;

-- Create schema
CREATE SCHEMA IF NOT EXISTS prod.sales;

-- Set default catalog/schema
USE CATALOG prod;
USE SCHEMA sales;
```

### 2. Storage Credentials & External Locations
```sql
-- Create storage credential (admin only)
CREATE STORAGE CREDENTIAL my_s3_cred
WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789:role/databricks-access');

-- Create external location
CREATE EXTERNAL LOCATION my_location
URL 's3://my-bucket/data/'
WITH (STORAGE CREDENTIAL my_s3_cred);

-- Grant access to external location
GRANT READ FILES ON EXTERNAL LOCATION my_location TO `data-team`;
```

### 3. Shares (Delta Sharing)
```sql
-- Create share for external data sharing
CREATE SHARE customer_data;

-- Add table to share
ALTER SHARE customer_data ADD TABLE prod.sales.customers;

-- Create recipient
CREATE RECIPIENT external_partner;

-- Grant share to recipient
GRANT SELECT ON SHARE customer_data TO RECIPIENT external_partner;
```

## Certification Exam Tips

### Databricks Certified Data Engineer Associate

**Focus areas:**
1. Unity Catalog hierarchy (40%)
2. Delta Lake operations (30%)
3. ETL with Spark SQL (20%)
4. Governance & security (10%)

**Key SQL to memorize:**
```sql
-- Must know
CREATE CATALOG / SCHEMA / TABLE
GRANT / REVOKE permissions
OPTIMIZE / VACUUM / ANALYZE
Time travel syntax (VERSION AS OF, TIMESTAMP AS OF)
MERGE statement
```

### Databricks Certified Data Engineer Professional

**Additional topics:**
1. Delta Live Tables (DLT)
2. Workflows & Jobs
3. Advanced Delta (change data feed, clone)
4. Performance tuning

## Quick Reference Commands

```sql
-- Show objects
SHOW CATALOGS;
SHOW SCHEMAS IN prod;
SHOW TABLES IN prod.sales;
SHOW GRANTS ON TABLE prod.sales.orders;

-- Describe objects
DESCRIBE CATALOG prod;
DESCRIBE SCHEMA prod.sales;
DESCRIBE TABLE prod.sales.orders;
DESCRIBE HISTORY prod.sales.orders;  -- Delta history
DESCRIBE DETAIL prod.sales.orders;   -- Storage info

-- Information schema queries
SELECT * FROM system.information_schema.tables;
SELECT * FROM system.information_schema.columns WHERE table_name = 'orders';
```

## Delta Live Tables (Quick Reference)

```python
import dlt

# Streaming Table (incremental)
@dlt.table(name="bronze_events")
def bronze():
    return spark.readStream.format("cloudFiles").load("/raw/")

# Materialized View (batch)
@dlt.table(name="silver_events")
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
def silver():
    return dlt.read("bronze_events").dropDuplicates()

# Gold aggregation
@dlt.table(name="gold_summary")
def gold():
    return dlt.read("silver_events").groupBy("date").count()
```

**Key Concepts:**
- `@dlt.table` = Materialized view (recomputes)
- `@dlt.expect` = Warn on failure
- `@dlt.expect_or_drop` = Drop failing rows
- `@dlt.expect_or_fail` = Fail pipeline
- `dlt.read()` = Batch read from DLT table
- `dlt.read_stream()` = Stream read from DLT table

See `references/delta-live-tables.md` for full documentation.

## Workflows (Quick Reference)

```json
{
  "name": "Daily ETL",
  "tasks": [
    {"task_key": "ingest", "notebook_task": {"notebook_path": "/ingest"}},
    {"task_key": "transform", "depends_on": [{"task_key": "ingest"}], "notebook_task": {"notebook_path": "/transform"}},
    {"task_key": "load", "depends_on": [{"task_key": "transform"}], "notebook_task": {"notebook_path": "/load"}}
  ],
  "schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "UTC"}
}
```

**Key Concepts:**
- **Job cluster**: Auto-terminates after job (cost-effective)
- **Task values**: Pass data between tasks via `dbutils.jobs.taskValues`
- **run_if**: Control task execution (`ALL_SUCCESS`, `ALL_DONE`, etc.)
- **Retries**: `max_retries` + `retry_on_timeout`

**Common Cron Schedules:**
| Schedule | Expression |
|----------|------------|
| Daily 6 AM | `0 0 6 * * ?` |
| Hourly | `0 0 * * * ?` |
| Every 15 min | `0 */15 * * * ?` |

See `references/workflows.md` for full documentation.

## Spark SQL (Quick Reference)

```sql
-- Window functions (CRITICAL FOR EXAM)
SELECT
    customer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_num,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total,
    LAG(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_amount
FROM orders;

-- CTE
WITH high_value AS (
    SELECT customer_id, SUM(amount) AS total
    FROM orders GROUP BY customer_id HAVING SUM(amount) > 1000
)
SELECT * FROM customers c JOIN high_value h ON c.id = h.customer_id;

-- MERGE (upsert)
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

**Key Window Functions:** ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE

See `references/spark-sql-patterns.md` for complete reference.

## Structured Streaming & Auto Loader (Quick Reference)

```python
# Auto Loader (recommended for file ingestion)
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schemas/events")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/data/landing/")
)

# Write stream with checkpoint
(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/events")
    .trigger(availableNow=True)  # Process all, then stop
    .table("bronze.events"))
```

**Key Concepts:**
- `cloudFiles` = Auto Loader format
- `schemaLocation` = Required for schema inference
- `schemaEvolutionMode` = Handle new columns (addNewColumns, rescue, fail)
- `checkpointLocation` = Required for exactly-once
- `trigger(availableNow=True)` = Incremental batch

See `references/structured-streaming.md` and `references/auto-loader.md` for full documentation.

## See Also

- `references/unity-catalog-permissions.md` - Deep dive on permissions
- `references/delta-lake-internals.md` - How Delta works under the hood
- `references/delta-live-tables.md` - DLT pipelines and expectations
- `references/workflows.md` - Jobs, tasks, scheduling, orchestration
- `references/spark-sql-patterns.md` - SQL, window functions, CTEs
- `references/structured-streaming.md` - Streaming, watermarks, triggers
- `references/auto-loader.md` - File ingestion, schema evolution
- `references/exam-questions.md` - Practice questions
