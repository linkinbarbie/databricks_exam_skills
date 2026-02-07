# Structured Streaming

## Core Concept

Structured Streaming treats a live data stream as an **unbounded table** that grows continuously. New data is appended as new rows to this table.

```
Input Stream (unbounded table):
┌──────────────────────────────────────────────┐
│  Existing Data  │  New Data (this trigger)    │
│  ████████████   │  ████                       │
└──────────────────────────────────────────────┘
        ↓ Process incrementally
┌──────────────────────────────────────────────┐
│              Result Table                      │
│  (updated with each trigger)                  │
└──────────────────────────────────────────────┘
        ↓ Output
┌──────────────────────────────────────────────┐
│              Output Sink                       │
│  (append / complete / update)                 │
└──────────────────────────────────────────────┘
```

## Basic Pattern

```python
# 1. Read stream
stream_df = (spark.readStream
    .format("source_format")
    .option("key", "value")
    .load("path"))

# 2. Transform (same as batch DataFrame API!)
result = stream_df.filter(stream_df.value > 0).groupBy("key").count()

# 3. Write stream
query = (result.writeStream
    .format("sink_format")
    .outputMode("mode")
    .option("checkpointLocation", "/checkpoint/path")
    .trigger(processingTime="10 seconds")
    .start())

# 4. Await termination
query.awaitTermination()
```

## Sources (Input)

### File Source

```python
# Read files as they appear
stream = (spark.readStream
    .format("parquet")  # or csv, json, orc, text
    .schema(my_schema)  # REQUIRED for file source
    .option("maxFilesPerTrigger", 100)
    .load("/data/landing/"))
```

### Kafka Source

```python
stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:9092,host2:9092")
    .option("subscribe", "topic1,topic2")         # Specific topics
    # .option("subscribePattern", "topic-.*")      # Pattern
    .option("startingOffsets", "latest")           # latest / earliest / JSON
    .option("failOnDataLoss", "false")
    .load())

# Kafka provides: key, value, topic, partition, offset, timestamp
# Parse value (typically JSON)
parsed = stream.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select("data.*")
```

### Rate Source (Testing)

```python
# Generates rows at specified rate
stream = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1000)
    .load())
# Provides: timestamp, value
```

### Delta Source

```python
stream = (spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 1000)
    .option("ignoreChanges", True)    # Ignore updates/deletes
    .load("/data/delta_table/"))
```

## Sinks (Output)

### File Sink

```python
query = (result.writeStream
    .format("parquet")  # or csv, json, orc, text
    .option("path", "/output/")
    .option("checkpointLocation", "/checkpoint/")
    .start())
```

### Kafka Sink

```python
# DataFrame must have 'value' column (and optional 'key', 'topic')
query = (result
    .select(F.to_json(F.struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host:9092")
    .option("topic", "output-topic")
    .option("checkpointLocation", "/checkpoint/")
    .start())
```

### Delta Sink

```python
query = (result.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoint/")
    .toTable("database.table_name"))
    # or .start("/output/delta/")
```

### Console Sink (Debugging)

```python
query = (result.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", False)
    .start())
```

### Foreach / ForeachBatch Sinks

```python
# ForeachBatch: process each micro-batch as a batch DataFrame
def process_batch(batch_df, batch_id):
    batch_df.write.mode("append").saveAsTable("results")
    # Can write to multiple sinks, do complex logic, etc.

query = (result.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/checkpoint/")
    .start())

# Foreach: process each row individually
class ForeachWriter:
    def open(self, partition_id, epoch_id):
        # Open connection
        return True
    def process(self, row):
        # Process single row
        pass
    def close(self, error):
        # Close connection
        pass

query = (result.writeStream
    .foreach(ForeachWriter())
    .start())
```

## Output Modes

| Mode | Behavior | Use With |
|------|----------|----------|
| **append** | Only NEW rows added to result table | No aggregations, or windowed aggregations with watermark |
| **complete** | Entire result table output each trigger | Aggregations (groupBy) |
| **update** | Only CHANGED rows output each trigger | Aggregations, maps |

```python
# Append (default) - for non-aggregation queries
stream.writeStream.outputMode("append")

# Complete - entire result table each time
stream.writeStream.outputMode("complete")

# Update - only changed rows
stream.writeStream.outputMode("update")
```

**Constraints:**
- `append` + `groupBy` = **error** (unless with watermark)
- `complete` + no aggregation = **error**
- File sinks only support `append`

## Triggers

```python
# Default: process as fast as possible (next batch starts after previous finishes)
.trigger()

# Fixed interval: process every 10 seconds
.trigger(processingTime="10 seconds")
.trigger(processingTime="1 minute")

# Once: single micro-batch, then stop (deprecated)
.trigger(once=True)

# Available now: process ALL available data, then stop
.trigger(availableNow=True)

# Continuous (experimental): ~1ms latency
.trigger(continuous="1 second")
```

| Trigger | Latency | Use Case |
|---------|---------|----------|
| Default | Medium | General streaming |
| `processingTime` | Medium | Rate-limited processing |
| `once=True` | N/A | One-time catch-up (deprecated) |
| `availableNow=True` | N/A | Incremental batch (replaces once) |
| `continuous` | ~1ms | Ultra-low latency (experimental) |

## Watermarks

Watermarks handle **late-arriving data** and manage state:

```python
# Watermark = "accept data up to 10 minutes late"
result = (stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        F.window("event_time", "5 minutes"),
        "device_id"
    )
    .count())
```

### How Watermarks Work

```
Current max event_time: 12:30
Watermark delay: 10 minutes
Watermark boundary: 12:20

Data with event_time >= 12:20 → ACCEPTED
Data with event_time <  12:20 → DROPPED (too late)

State for windows ending before 12:20 → CLEANED UP
```

### Watermark Rules

1. **Must be on event-time column** (not processing time)
2. **Required for windowed aggregations** in append mode
3. **Determines state cleanup** (how long to keep old state)
4. **Late data before watermark**: accepted
5. **Late data after watermark**: dropped

```python
# Without watermark: state grows forever (memory leak!)
# With watermark: old state cleaned up automatically

# Watermark MUST come before aggregation
stream \
    .withWatermark("event_time", "10 minutes") \  # Before groupBy
    .groupBy(F.window("event_time", "5 minutes")).count()
```

## Windowed Aggregations

### Tumbling Windows (Non-Overlapping)

```python
# 5-minute windows: [0:00-0:05), [0:05-0:10), ...
result = (stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "5 minutes"))
    .count())
```

### Sliding Windows (Overlapping)

```python
# 10-minute windows, sliding every 5 minutes
# [0:00-0:10), [0:05-0:15), [0:10-0:20), ...
result = (stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "10 minutes", "5 minutes"))
    .count())
```

### Session Windows (Gap-Based)

```python
# New window starts after 5 minutes of inactivity per key
result = (stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        F.session_window("event_time", "5 minutes"),
        "user_id"
    )
    .count())
```

## Stream-Stream Joins

```python
# Both streams need watermarks for state management
orders = (spark.readStream.format("delta").load("/orders/")
    .withWatermark("order_time", "1 hour"))

payments = (spark.readStream.format("delta").load("/payments/")
    .withWatermark("payment_time", "2 hours"))

# Inner join with time constraint
result = orders.join(
    payments,
    F.expr("""
        order_id = payment_order_id AND
        payment_time >= order_time AND
        payment_time <= order_time + INTERVAL 1 HOUR
    """)
)

# Left outer join (supported with watermark + time constraint)
result = orders.join(
    payments,
    F.expr("""
        order_id = payment_order_id AND
        payment_time >= order_time AND
        payment_time <= order_time + INTERVAL 1 HOUR
    """),
    "left_outer"
)
```

### Join Support Matrix

| Join Type | Without Watermark | With Watermark |
|-----------|-------------------|----------------|
| Inner | State grows forever | State cleaned up |
| Left Outer | Not supported | Supported |
| Right Outer | Not supported | Supported |
| Full Outer | Not supported | Not supported |

## Stream-Static Joins

```python
# Static DataFrame (read once, not updated)
countries = spark.read.parquet("/data/countries/")

# Stream joined with static table (no watermark needed)
result = stream_df.join(countries, "country_code")
# Left join: stream on left, static on right
result = stream_df.join(countries, "country_code", "left")
```

## Checkpointing

Checkpoints store streaming state for fault tolerance:

```python
query = (result.writeStream
    .option("checkpointLocation", "/checkpoint/my_query/")
    .start())
```

**Checkpoint contains:**
- Current offsets (which data has been processed)
- State data (for aggregations, joins)
- Commit log (which batches completed)

**Rules:**
- **One checkpoint per query** (don't share between queries)
- **Must be on reliable storage** (HDFS, S3, ADLS)
- **Don't delete checkpoints** (breaks exactly-once guarantee)
- **Changing query may break compatibility** with existing checkpoint

## Exactly-Once Semantics

Structured Streaming provides **exactly-once** processing:

1. **Sources are replayable**: Can re-read data from offset (Kafka, files)
2. **Sinks are idempotent**: Re-writing same batch produces same result
3. **Checkpoints**: Track offsets and state

```
Checkpoint + Replayable Source + Idempotent Sink = Exactly-Once
```

## Managing Streaming Queries

```python
# Get active queries
spark.streams.active

# Get query status
query.status
query.lastProgress    # Detailed metrics
query.recentProgress  # List of recent metrics

# Stop query
query.stop()

# Wait for termination
query.awaitTermination()
query.awaitTermination(timeout=60)  # With timeout (seconds)
```

## Deduplication

```python
# Deduplicate within a watermark window
result = (stream
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"]))

# Without watermark: keeps ALL seen IDs in state (memory grows!)
# With watermark: only keeps IDs within watermark window
```

## Key Exam Concepts

1. **Structured Streaming = unbounded table model**
2. **Output modes**: append (new rows), complete (all rows), update (changed rows)
3. **`availableNow=True` replaces `once=True`** for incremental batch
4. **Watermarks**: handle late data + manage state cleanup
5. **Watermark must come before aggregation** in the query
6. **Without watermark, state grows forever** (memory leak)
7. **Checkpoints are required** for fault tolerance and exactly-once
8. **Don't share checkpoints** between different queries
9. **Stream-stream joins require watermarks** on both sides
10. **Stream-static joins don't need watermarks**
11. **File sinks only support append mode**
12. **`foreachBatch` gives you a batch DataFrame** for each micro-batch
