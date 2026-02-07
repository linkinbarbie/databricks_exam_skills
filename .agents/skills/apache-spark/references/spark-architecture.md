# Spark Architecture Deep Dive

## Cluster Architecture

### Driver

The driver is the process that runs the `main()` function of your application:

- Creates `SparkSession` / `SparkContext`
- Converts user code into a **DAG** (Directed Acyclic Graph) of stages and tasks
- Negotiates resources with the cluster manager
- Schedules tasks on executors
- Tracks task status and handles failures
- Collects results from actions (e.g., `collect()`, `count()`)

**Driver failure = entire application fails.**

```python
# Driver memory (important for collect(), broadcast)
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.driver.maxResultSize", "2g")  # Max size for collect()
```

### Executors

Executors are JVM processes on worker nodes:

- Execute tasks assigned by the driver
- Store data in memory or disk for caching
- Report task status back to driver
- One executor per worker node per application (by default)

```python
# Executor configuration
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.executor.instances", "10")
```

**Executor Memory Layout:**
```
┌─────────────────────────────────────────────────┐
│              spark.executor.memory (e.g., 8g)    │
│                                                   │
│  ┌─────────────────────────────────────────────┐ │
│  │     Unified Memory (spark.memory.fraction)   │ │
│  │              Default: 0.6 (60%)              │ │
│  │                                               │ │
│  │  ┌───────────────┐  ┌─────────────────────┐ │ │
│  │  │   Execution   │  │     Storage         │ │ │
│  │  │   (shuffles,  │◄►│     (cache,         │ │ │
│  │  │    joins,     │  │      broadcast)     │ │ │
│  │  │    sorts)     │  │                     │ │ │
│  │  └───────────────┘  └─────────────────────┘ │ │
│  │       Can borrow from each other             │ │
│  └─────────────────────────────────────────────┘ │
│                                                   │
│  ┌─────────────────────────────────────────────┐ │
│  │     User Memory (1 - spark.memory.fraction)  │ │
│  │     Default: 0.4 (40%)                       │ │
│  │     (User data structures, UDF variables)    │ │
│  └─────────────────────────────────────────────┘ │
│                                                   │
│  ┌─────────────────────────────────────────────┐ │
│  │     Reserved Memory: 300MB (fixed)           │ │
│  └─────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘

Off-Heap: spark.executor.memoryOverhead (default 10% or 384MB)
  - Python processes (PySpark), JVM overhead, network buffers
```

**Key:** Execution and storage share unified memory. Execution can evict storage (but not vice versa by default).

### Cluster Managers

| Manager | Description | Best For |
|---------|-------------|----------|
| **Standalone** | Built-in, simple | Dev, small clusters |
| **YARN** | Hadoop resource manager | Hadoop-integrated environments |
| **Mesos** | General-purpose (deprecated in Spark 3.2+) | Legacy |
| **Kubernetes** | Container orchestration | Cloud-native, dynamic scaling |

```bash
# Standalone
spark-submit --master spark://host:7077 app.py

# YARN
spark-submit --master yarn --deploy-mode cluster app.py

# Kubernetes
spark-submit --master k8s://https://k8s-api:443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=spark:latest app.py
```

## Job Execution Model

### DAG (Directed Acyclic Graph)

When you call an **action**, Spark builds a DAG of all transformations needed:

```python
# Example
rdd = sc.textFile("data.txt")          # Stage 0
    .flatMap(lambda x: x.split(" "))   # Stage 0 (narrow)
    .map(lambda x: (x, 1))            # Stage 0 (narrow)
    .reduceByKey(lambda a, b: a + b)   # Stage 1 (wide - shuffle!)
    .collect()                          # ACTION triggers execution
```

### Stages

Stages are separated by **shuffle boundaries** (wide transformations):

```
Job
├── Stage 0 (before shuffle)
│   ├── Task 0 (partition 0): textFile → flatMap → map
│   ├── Task 1 (partition 1): textFile → flatMap → map
│   └── Task 2 (partition 2): textFile → flatMap → map
│
│   ── SHUFFLE (reduceByKey) ──
│
└── Stage 1 (after shuffle)
    ├── Task 0: reduceByKey → collect
    └── Task 1: reduceByKey → collect
```

### Tasks

- One task per partition per stage
- Smallest unit of execution
- Runs on a single core of a single executor
- Processes one partition of data

```
Number of tasks = Number of partitions × Number of stages
```

### Pipelining

Within a stage, Spark **pipelines** narrow transformations:
- Data flows through all narrow transforms without writing to disk
- Each record passes through the entire pipeline before the next record
- This is why narrow transforms are efficient

## Deployment Modes

### Client Mode
```
┌─────────────┐       ┌──────────────────┐
│ Your Machine│       │    Cluster       │
│             │       │                  │
│  ┌────────┐ │       │  ┌──────────┐   │
│  │ DRIVER │ │◄─────►│  │ Executor │   │
│  └────────┘ │       │  │ Executor │   │
│             │       │  │ Executor │   │
└─────────────┘       └──────────────────┘
```
- Driver runs on **your machine** (submitting machine)
- Good for interactive use (notebooks, spark-shell)
- If your machine disconnects, job fails

### Cluster Mode
```
┌──────────────────────────────────────┐
│              Cluster                  │
│                                      │
│  ┌────────┐  ┌──────────┐           │
│  │ DRIVER │  │ Executor │           │
│  └────────┘  │ Executor │           │
│              │ Executor │           │
└──────────────────────────────────────┘
```
- Driver runs **inside the cluster** on a worker node
- Good for production jobs
- Job continues even if submitting machine disconnects

## Dynamic Allocation

Automatically scales executors based on workload:

```python
spark.conf.set("spark.dynamicAllocation.enabled", True)
spark.conf.set("spark.dynamicAllocation.minExecutors", 2)
spark.conf.set("spark.dynamicAllocation.maxExecutors", 100)
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
```

**How it works:**
1. Pending tasks in queue → request more executors
2. Executor idle for `executorIdleTimeout` → release executor
3. Scales between `minExecutors` and `maxExecutors`

## Speculative Execution

Re-launches slow tasks on other executors:

```python
spark.conf.set("spark.speculation", True)
spark.conf.set("spark.speculation.multiplier", 1.5)  # 1.5x slower than median
spark.conf.set("spark.speculation.quantile", 0.75)   # After 75% tasks complete
```

**Use when:** Heterogeneous cluster, some nodes slower than others.
**Avoid when:** Tasks have side effects (writes to external systems).

## Fault Tolerance

### RDD Lineage
- Each RDD tracks its **lineage** (chain of transformations)
- If a partition is lost, Spark recomputes it from the lineage
- Only the lost partition is recomputed, not the entire RDD

### Checkpointing
```python
# Set checkpoint directory
sc.setCheckpointDir("/checkpoint/")

# Checkpoint an RDD (breaks lineage, saves to reliable storage)
rdd.checkpoint()

# For DataFrames, use localCheckpoint (faster, less reliable)
df.localCheckpoint()
```

### Task Retry
```python
# Number of task retries before failing the stage
spark.conf.set("spark.task.maxFailures", 4)  # Default: 4
```

## Key Exam Concepts

1. **Driver creates the DAG, not executors**
2. **Shuffles create stage boundaries**
3. **One task per partition per stage**
4. **Client mode: driver on submitting machine; Cluster mode: driver in cluster**
5. **Executor memory is shared between execution and storage**
6. **Dynamic allocation scales executors, not cores**
7. **RDD lineage enables fault tolerance without replication**
8. **Narrow transforms pipeline within a stage (no disk write)**
