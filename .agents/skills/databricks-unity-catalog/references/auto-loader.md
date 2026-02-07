# Auto Loader Deep Dive

## What is Auto Loader?

Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage.

```
┌─────────────────────────────────────────────────────────────────┐
│                         AUTO LOADER                              │
│                                                                  │
│   Cloud Storage              Auto Loader            Delta Table  │
│   ┌─────────────┐           ┌──────────┐          ┌──────────┐  │
│   │ file1.json  │──────────▶│ Discover │─────────▶│  Bronze  │  │
│   │ file2.json  │  NEW      │ & Ingest │ Process  │  Table   │  │
│   │ file3.json ★│  FILES    │          │          │          │  │
│   └─────────────┘           └──────────┘          └──────────┘  │
│                                   │                              │
│                                   ▼                              │
│                            ┌──────────┐                          │
│                            │Checkpoint│ (tracks processed files) │
│                            └──────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

## Why Auto Loader?

| Challenge | Auto Loader Solution |
|-----------|---------------------|
| Millions of files | Efficient file discovery |
| New files arriving | Incremental processing |
| Schema changes | Schema evolution |
| Exactly-once | Checkpoint-based tracking |
| Late files | Rescans on schedule |

## Basic Syntax

### Python

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schemas/my_table")
    .load("/data/landing/")
)

# Write to Delta
(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/my_table")
    .trigger(availableNow=True)
    .table("bronze.my_table"))
```

### SQL

```sql
-- COPY INTO (batch alternative)
COPY INTO bronze.my_table
FROM '/data/landing/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
```

## File Discovery Modes

### Directory Listing (Default)

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "false")  # Default
    .load("/data/landing/")
)
```

- Lists directory contents each trigger
- Good for: Small-medium file counts
- Latency: Depends on listing time

### Notification Mode (Recommended for Scale)

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.resourceGroup", "my-resource-group")  # Azure
    .load("abfss://container@storage.dfs.core.windows.net/path/")
)
```

- Uses cloud events (S3 SQS, Azure Event Grid, GCS Pub/Sub)
- Good for: Millions of files
- Latency: Near real-time

| Cloud | Notification Service |
|-------|---------------------|
| AWS | S3 Events → SQS |
| Azure | Blob Events → Event Grid |
| GCP | GCS Events → Pub/Sub |

## Schema Management

### Schema Inference

```python
# Infer schema from first files
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")  # Infer types
    .option("cloudFiles.schemaLocation", "/schemas/my_table")  # Store schema
    .load("/data/landing/")
)
```

### Schema Hints

```python
# Provide hints for specific columns
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaHints", "id INT, amount DOUBLE, timestamp TIMESTAMP")
    .option("cloudFiles.schemaLocation", "/schemas/my_table")
    .load("/data/landing/")
)
```

### Explicit Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True)
])

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema)  # Explicit schema
    .load("/data/landing/")
)
```

## Schema Evolution

### Evolution Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `addNewColumns` | Add new columns automatically | Evolving sources |
| `rescue` | Put unknown in `_rescued_data` | Catch unexpected |
| `failOnNewColumns` | Fail if new columns | Strict control |
| `none` | Ignore new columns | Fixed schema |

```python
# Add new columns automatically
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schemas/my_table")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/data/landing/")
)
```

### Rescue Mode

```python
# Capture problematic records
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schemas/my_table")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load("/data/landing/")
)

# df now has column: _rescued_data STRING
# Contains JSON of fields that didn't match schema
```

## Format-Specific Options

### JSON

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiLine", "true")  # Multi-line JSON
    .option("primitivesAsString", "false")  # Infer numeric types
    .option("allowUnquotedFieldNames", "true")
    .load("/data/")
)
```

### CSV

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", "\\")
    .option("nullValue", "NULL")
    .load("/data/")
)
```

### Parquet

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/schemas/my_table")
    .load("/data/")
)
```

### Avro

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "avro")
    .option("avroSchema", avro_schema_string)  # Optional explicit schema
    .load("/data/")
)
```

## Performance Tuning

### Max Files Per Trigger

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.maxFilesPerTrigger", 1000)  # Process 1000 files per batch
    .load("/data/")
)
```

### Max Bytes Per Trigger

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.maxBytesPerTrigger", "10g")  # 10 GB per batch
    .load("/data/")
)
```

### Backfill Parallelism

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.backfillInterval", "1 day")  # Rescan for missed files
    .load("/data/")
)
```

## File Metadata Columns

Auto Loader provides metadata about source files:

```python
from pyspark.sql.functions import col, input_file_name

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("/data/")
    .select(
        "*",
        "_metadata.file_path",        # Full file path
        "_metadata.file_name",        # File name only
        "_metadata.file_size",        # Size in bytes
        "_metadata.file_modification_time"  # Last modified
    )
)

# Or use input_file_name() function
df = df.withColumn("source_file", input_file_name())
```

## Error Handling

### Corrupt Records

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("badRecordsPath", "/data/bad_records/")  # Write bad records here
    .option("columnNameOfCorruptRecord", "_corrupt_record")  # Add column
    .load("/data/")
)
```

### Ignore Corrupt Files

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("ignoreCorruptFiles", "true")
    .option("ignoreMissingFiles", "true")
    .load("/data/")
)
```

## Complete Example: Production Pipeline

```python
from pyspark.sql.functions import current_timestamp, input_file_name

# Configure Auto Loader
raw_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schemas/events")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.maxFilesPerTrigger", 1000)
    .option("cloudFiles.useNotifications", "true")
    .load("s3://my-bucket/landing/events/")
)

# Add metadata columns
enriched_df = (
    raw_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

# Write to Bronze table
query = (
    enriched_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/events_bronze")
    .option("mergeSchema", "true")  # Allow schema evolution in Delta
    .trigger(availableNow=True)  # Process all available, then stop
    .table("bronze.events")
)

query.awaitTermination()
print(f"Processed {query.lastProgress['numInputRows']} rows")
```

## Auto Loader vs COPY INTO

| Feature | Auto Loader | COPY INTO |
|---------|-------------|-----------|
| Processing | Streaming | Batch |
| File tracking | Automatic (checkpoint) | Manual (COPY_OPTIONS) |
| Schema evolution | Built-in | Manual |
| Large file count | Efficient (notifications) | Can be slow |
| Exactly-once | Guaranteed | With COPY_OPTIONS |
| Use case | Continuous ingestion | One-time loads |

```sql
-- COPY INTO equivalent
COPY INTO bronze.events
FROM 's3://my-bucket/landing/events/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('inferSchema' = 'true', 'multiLine' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
```

## Exam Tips

1. **Auto Loader = cloudFiles format** (not a separate tool)
2. **Schema location is required** for schema inference
3. **Notification mode** for millions of files
4. **_rescued_data** column with rescue mode
5. **checkpointLocation** tracks processed files
6. **trigger(availableNow=True)** for incremental batch
7. **COPY INTO** is the batch alternative
