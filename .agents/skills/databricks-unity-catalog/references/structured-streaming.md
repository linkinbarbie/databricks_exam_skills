# Structured Streaming & Auto Loader

## What is Structured Streaming?

Structured Streaming treats a **live data stream as a table** that is continuously appended. You write batch-like queries that run incrementally.

```
┌─────────────────────────────────────────────────────────────────┐
│                    STRUCTURED STREAMING                          │
│                                                                  │
│   Input Stream          Unbounded Table         Output Sink     │
│   ┌─────────┐          ┌─────────────┐         ┌─────────┐     │
│   │ event 1 │────┐     │ ┌─────────┐ │         │ Results │     │
│   │ event 2 │────┼────▶│ │ event 1 │ │────────▶│ (Delta) │     │
│   │ event 3 │────┘     │ │ event 2 │ │         └─────────┘     │
│   │   ...   │          │ │ event 3 │ │                          │
│   └─────────┘          │ │   ...   │ │                          │
│                        │ └─────────┘ │                          │
│                        └─────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

## Basic Streaming Query

### Read Stream

```python
# Read from Kafka
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host:port")
    .option("subscribe", "topic")
    .load()
)

# Read from Delta table
stream_df = (
    spark.readStream
    .format("delta")
    .table("bronze.events")
)

# Read from files (Auto Loader)
stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("/data/incoming/")
)
```

### Transform

```python
# Same operations as batch!
transformed_df = (
    stream_df
    .filter(col("event_type") == "purchase")
    .select("user_id", "amount", "timestamp")
    .withColumn("processed_at", current_timestamp())
)
```

### Write Stream

```python
# Write to Delta table
query = (
    transformed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/my_stream")
    .table("silver.purchases")
)

# Start and wait
query.awaitTermination()
```

## Auto Loader (cloudFiles)

Auto Loader is the **recommended way** to ingest files in Databricks.

### Basic Auto Loader

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schemas/events")
    .load("/data/landing/events/")
)
```

### Auto Loader Options

| Option | Description |
|--------|-------------|
| `cloudFiles.format` | File format (json, csv, parquet, avro) |
| `cloudFiles.schemaLocation` | Where to store inferred schema |
| `cloudFiles.inferColumnTypes` | Infer types (default: false for JSON) |
| `cloudFiles.schemaEvolutionMode` | Handle new columns |
| `cloudFiles.maxFilesPerTrigger` | Files per batch |
| `cloudFiles.useNotifications` | Use cloud notifications (faster) |

### Schema Evolution

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schemas/events")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Auto-add new columns
    .load("/data/landing/")
)
```

**Schema Evolution Modes:**

| Mode | Behavior |
|------|----------|
| `addNewColumns` | Add new columns automatically |
| `rescue` | Put unknown fields in `_rescued_data` column |
| `failOnNewColumns` | Fail if new columns appear |
| `none` | Ignore new columns |

### Rescued Data Column

```python
# Capture malformed records
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schemas/events")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load("/data/landing/")
)

# Schema includes: ..., _rescued_data STRING
# Malformed records go to _rescued_data instead of failing
```

## Output Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `append` | Only new rows | Insert-only, no aggregations |
| `complete` | Entire result | Aggregations (full table output) |
| `update` | Changed rows only | Aggregations with watermark |

```python
# Append mode (most common)
query = (
    df.writeStream
    .outputMode("append")
    .format("delta")
    .table("output")
)

# Complete mode (for aggregations)
agg_df = df.groupBy("category").count()
query = (
    agg_df.writeStream
    .outputMode("complete")
    .format("delta")
    .table("category_counts")
)

# Update mode (aggregations with watermark)
query = (
    agg_df.writeStream
    .outputMode("update")
    .format("delta")
    .table("category_counts")
)
```

## Triggers

| Trigger | Description |
|---------|-------------|
| `processingTime("10 seconds")` | Micro-batch every 10s |
| `once=True` | Single batch, then stop |
| `availableNow=True` | Process all available, then stop |
| `continuous="1 second"` | Low-latency continuous (experimental) |

```python
# Process every 10 seconds
query = (
    df.writeStream
    .trigger(processingTime="10 seconds")
    .format("delta")
    .table("output")
)

# Process once (for incremental batch)
query = (
    df.writeStream
    .trigger(once=True)
    .format("delta")
    .table("output")
)

# Process all available data, then stop
query = (
    df.writeStream
    .trigger(availableNow=True)
    .format("delta")
    .table("output")
)
```

## Watermarks (Late Data Handling)

Watermarks define how long to wait for late data.

```python
from pyspark.sql.functions import window

# Define watermark: accept data up to 10 minutes late
windowed_counts = (
    df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window("event_time", "5 minutes"),  # 5-minute tumbling window
        "user_id"
    )
    .count()
)

# Write with update mode
query = (
    windowed_counts.writeStream
    .outputMode("update")
    .format("delta")
    .option("checkpointLocation", "/checkpoints/windowed")
    .table("user_activity_5min")
)
```

### Window Types

```python
# Tumbling window (non-overlapping)
window("timestamp", "5 minutes")
# Windows: [00:00-00:05), [00:05-00:10), ...

# Sliding window (overlapping)
window("timestamp", "10 minutes", "5 minutes")
# Windows: [00:00-00:10), [00:05-00:15), [00:10-00:20), ...

# Session window (gap-based)
session_window("timestamp", "10 minutes")
# New window starts after 10 min of inactivity
```

## Stateful Operations

### Aggregations

```python
# Running count (requires complete or update mode)
df.groupBy("category").count()

# Running sum
df.groupBy("user_id").agg(sum("amount"))

# With watermark (allows update mode)
(df
    .withWatermark("timestamp", "1 hour")
    .groupBy("user_id")
    .agg(sum("amount")))
```

### Deduplication

```python
# Deduplicate within watermark
deduplicated = (
    df
    .withWatermark("timestamp", "10 minutes")
    .dropDuplicates(["event_id"])
)
```

### Stream-Stream Joins

```python
# Join two streams
impressions = spark.readStream.table("impressions")
clicks = spark.readStream.table("clicks")

# Must have watermarks for stream-stream join
impressions_with_wm = impressions.withWatermark("imp_time", "2 hours")
clicks_with_wm = clicks.withWatermark("click_time", "3 hours")

joined = (
    impressions_with_wm.join(
        clicks_with_wm,
        expr("""
            imp_id = click_imp_id AND
            click_time >= imp_time AND
            click_time <= imp_time + interval 1 hour
        """),
        "leftOuter"
    )
)
```

### Stream-Static Joins

```python
# Join stream with static table (no watermark needed)
stream_df = spark.readStream.table("orders")
static_df = spark.read.table("customers")

joined = stream_df.join(static_df, "customer_id", "left")
```

## foreachBatch (Custom Sinks)

```python
def process_batch(batch_df, batch_id):
    # Custom processing for each micro-batch
    batch_df.write.mode("append").saveAsTable("target")

    # Additional operations
    batch_df.write.format("jdbc").save()

    # Metrics
    print(f"Batch {batch_id}: {batch_df.count()} rows")

query = (
    df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/checkpoints/custom")
    .start()
)
```

### MERGE with foreachBatch

```python
from delta.tables import DeltaTable

def upsert_to_delta(batch_df, batch_id):
    delta_table = DeltaTable.forName(spark, "target_table")

    delta_table.alias("target").merge(
        batch_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

query = (
    df.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/checkpoints/upsert")
    .start()
)
```

## Checkpoints

Checkpoints store streaming state for fault tolerance.

```python
# Always specify checkpoint location
query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/my_stream")  # Required!
    .table("output")
)
```

**Checkpoint contains:**
- Current offset (where in stream)
- State data (aggregations, deduplication)
- Committed data (what's been written)

**Important:**
- Each stream needs its **own** checkpoint directory
- Don't delete checkpoints for running streams
- Moving checkpoints requires stream restart

## Monitoring Streams

### Query Object

```python
query = df.writeStream.start()

# Check status
print(query.status)
print(query.recentProgress)
print(query.lastProgress)

# Check if running
print(query.isActive)

# Stop query
query.stop()

# Wait for termination
query.awaitTermination()
query.awaitTermination(timeout=3600)  # 1 hour timeout
```

### Stream Metrics

```python
# Last progress contains metrics
progress = query.lastProgress

print(f"Input rows: {progress['numInputRows']}")
print(f"Processing time: {progress['triggerExecution']['durationMs']}")
print(f"Batch ID: {progress['batchId']}")
```

### Streaming Query Listener

```python
class MyListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        print(f"Progress: {event.progress.numInputRows} rows")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")

spark.streams.addListener(MyListener())
```

## Common Patterns

### Bronze-Silver-Gold Streaming

```python
# Bronze: Raw ingestion
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schemas/bronze")
    .load("/landing/events/")
)

bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/bronze")
    .table("bronze.events")
)

# Silver: Cleaned (reads from Bronze)
silver_stream = (
    spark.readStream
    .table("bronze.events")
    .filter(col("event_id").isNotNull())
    .dropDuplicates(["event_id"])
)

silver_query = (
    silver_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/silver")
    .table("silver.events")
)

# Gold: Aggregated (reads from Silver)
gold_stream = (
    spark.readStream
    .table("silver.events")
    .withWatermark("event_time", "1 hour")
    .groupBy(window("event_time", "1 hour"), "category")
    .agg(count("*").alias("event_count"))
)

gold_query = (
    gold_stream.writeStream
    .outputMode("update")
    .format("delta")
    .option("checkpointLocation", "/checkpoints/gold")
    .table("gold.hourly_events")
)
```

### Incremental Batch (trigger once)

```python
# Process new files, then stop
query = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .load("/data/incoming/")
    .writeStream
    .trigger(availableNow=True)  # Process all available, stop
    .format("delta")
    .option("checkpointLocation", "/checkpoints/incremental")
    .table("target")
)

query.awaitTermination()
```

## Exam Questions

### Q1: Auto Loader vs COPY INTO
**When should you use Auto Loader vs COPY INTO?**

- **Auto Loader**: Streaming, continuous ingestion, schema evolution
- **COPY INTO**: One-time batch load, simpler setup

### Q2: Output Modes
**Which output mode is required for streaming aggregations without watermark?**

`complete` mode (outputs entire result table each trigger)

### Q3: Checkpoints
**What happens if you delete a checkpoint directory?**

Stream restarts from beginning, may cause duplicate processing

### Q4: Watermarks
**What does a watermark of "10 minutes" mean?**

Data arriving more than 10 minutes late (based on event time) may be dropped

### Q5: Schema Evolution
**How does Auto Loader handle new columns in source data?**

Use `cloudFiles.schemaEvolutionMode` = `addNewColumns` to auto-add new columns

### Q6: Trigger Once
**How do you process all available data and then stop?**

Use `.trigger(availableNow=True)` or `.trigger(once=True)`

### Q7: Stream-Stream Join
**What's required for joining two streams?**

Both streams must have watermarks defined

## Key Commands Summary

```python
# Auto Loader
spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load(path)

# Read Delta stream
spark.readStream.table("table_name")

# Write stream
df.writeStream.format("delta").option("checkpointLocation", path).table("target")

# Triggers
.trigger(processingTime="10 seconds")
.trigger(once=True)
.trigger(availableNow=True)

# Watermark
df.withWatermark("timestamp", "10 minutes")

# Window
window("timestamp", "5 minutes")  # Tumbling
window("timestamp", "10 minutes", "5 minutes")  # Sliding

# Output modes
.outputMode("append")
.outputMode("complete")
.outputMode("update")

# Query management
query.awaitTermination()
query.stop()
query.status
query.lastProgress
```
