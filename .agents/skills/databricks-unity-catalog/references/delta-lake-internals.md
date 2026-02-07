# Delta Lake Internals

## How Delta Lake Works

### Transaction Log (_delta_log)

Every Delta table has a `_delta_log/` directory containing JSON files:

```
my_table/
├── _delta_log/
│   ├── 00000000000000000000.json  # Version 0
│   ├── 00000000000000000001.json  # Version 1
│   ├── 00000000000000000002.json  # Version 2
│   └── 00000000000000000010.checkpoint.parquet  # Checkpoint
├── part-00000-xxx.parquet
├── part-00001-xxx.parquet
└── part-00002-xxx.parquet
```

**Each JSON file contains:**
- `add`: New files added
- `remove`: Files marked for deletion
- `metaData`: Schema changes
- `commitInfo`: Who, when, operation

### Checkpoints

Every 10 commits, Delta creates a checkpoint:
- Parquet file summarizing all actions
- Speeds up reads (don't need to replay all JSON)

```sql
-- See checkpoint interval
DESCRIBE DETAIL my_table;
```

## ACID Properties

| Property | How Delta Implements |
|----------|---------------------|
| **Atomicity** | All or nothing via transaction log |
| **Consistency** | Schema enforcement, constraints |
| **Isolation** | Optimistic concurrency control |
| **Durability** | Data written to object storage |

## File Organization

### Compaction (OPTIMIZE)

```sql
-- Before: Many small files
-- my_table/
-- ├── part-00000-xxx.parquet  (1 MB)
-- ├── part-00001-xxx.parquet  (2 MB)
-- ├── part-00002-xxx.parquet  (500 KB)
-- └── ... (1000 files)

OPTIMIZE my_table;

-- After: Fewer large files (~1GB each)
-- my_table/
-- ├── part-00000-xxx.parquet  (1 GB)
-- └── part-00001-xxx.parquet  (500 MB)
```

### Z-Ordering

Colocates related data for faster queries:

```sql
-- Optimize for queries filtering on customer_id and date
OPTIMIZE my_table ZORDER BY (customer_id, order_date);

-- Now this query reads fewer files:
SELECT * FROM my_table WHERE customer_id = 123 AND order_date = '2024-01-01';
```

### Liquid Clustering (New)

Automatic, incremental clustering:

```sql
-- Create table with liquid clustering
CREATE TABLE my_table (
    id INT,
    customer_id INT,
    order_date DATE
) CLUSTER BY (customer_id, order_date);

-- Data automatically clustered on writes
```

## Time Travel

### How It Works

Delta keeps old Parquet files until VACUUM runs:

```
Version 1: file_a.parquet, file_b.parquet
Version 2: file_a.parquet, file_c.parquet (file_b removed, file_c added)
Version 3: file_d.parquet (file_a, file_c removed)
```

Querying Version 1 reads file_a + file_b (still on disk).

### Retention

```sql
-- Default retention: 7 days
VACUUM my_table;  -- Removes files older than 7 days

-- Custom retention
VACUUM my_table RETAIN 720 HOURS;  -- 30 days

-- ⚠️ Never go below 7 days in production
-- VACUUM my_table RETAIN 0 HOURS;  -- Dangerous!
```

## Change Data Feed (CDF)

Track row-level changes:

```sql
-- Enable CDF
ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Read changes
SELECT * FROM table_changes('my_table', 1, 5);
-- Returns: _change_type (insert/update_preimage/update_postimage/delete)
```

## Performance Tuning

### Statistics

```sql
-- Collect statistics for query optimization
ANALYZE TABLE my_table COMPUTE STATISTICS;
ANALYZE TABLE my_table COMPUTE STATISTICS FOR COLUMNS customer_id, amount;
```

### Data Skipping

Delta automatically skips files based on min/max statistics:

```sql
-- This query skips files where min(amount) > 1000 or max(amount) < 1000
SELECT * FROM orders WHERE amount = 1000;
```

### Bloom Filters

For high-cardinality columns:

```sql
-- Create bloom filter index
CREATE BLOOMFILTER INDEX ON TABLE my_table FOR COLUMNS (transaction_id);

-- Speeds up point lookups
SELECT * FROM my_table WHERE transaction_id = 'abc123';
```

## Key Exam Concepts

1. **Transaction log is the source of truth**
2. **OPTIMIZE compacts small files**
3. **Z-ORDER colocates data for filtered queries**
4. **VACUUM removes old files (respect retention!)**
5. **Time travel requires files to exist (not vacuumed)**
6. **Change Data Feed tracks row-level changes**
