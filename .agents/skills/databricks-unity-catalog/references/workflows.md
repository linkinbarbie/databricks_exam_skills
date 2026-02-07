# Databricks Workflows

## What are Workflows?

Workflows is Databricks' **orchestration service** for scheduling and running jobs. A job can contain multiple tasks with dependencies.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              WORKFLOW (JOB)                                  │
│                                                                              │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐                              │
│   │  TASK 1  │───▶│  TASK 2  │───▶│  TASK 3  │                              │
│   │ (ingest) │    │(transform)│    │ (load)   │                              │
│   └──────────┘    └────┬─────┘    └──────────┘                              │
│                        │                                                     │
│                        ▼                                                     │
│                   ┌──────────┐                                               │
│                   │  TASK 4  │                                               │
│                   │ (notify) │                                               │
│                   └──────────┘                                               │
│                                                                              │
│   Schedule: Daily at 6 AM UTC                                                │
│   Cluster: Job cluster (auto-terminates)                                     │
│   Alerts: Email on failure                                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Concepts

### Job vs Task

| Concept | Description |
|---------|-------------|
| **Job** | Container for one or more tasks with schedule and settings |
| **Task** | Single unit of work (notebook, script, DLT pipeline, etc.) |
| **Run** | Single execution of a job |
| **Cluster** | Compute that runs the tasks |

### Task Types

| Type | Description | Use Case |
|------|-------------|----------|
| **Notebook** | Run a Databricks notebook | Most common, flexible |
| **Python Script** | Run a .py file | Standalone scripts |
| **Python Wheel** | Run packaged Python | Production code |
| **JAR** | Run Java/Scala JAR | Spark applications |
| **SQL** | Run SQL queries | Data transformations |
| **DLT Pipeline** | Trigger Delta Live Tables | Managed ETL |
| **dbt** | Run dbt project | SQL transformations |
| **Spark Submit** | Generic Spark job | Legacy jobs |
| **If/Else** | Conditional branching | Dynamic workflows |
| **For Each** | Loop over values | Parameterized runs |

## Creating Jobs

### UI Method

1. **Workflows** → **Create Job**
2. Add tasks and define dependencies
3. Configure schedule and cluster
4. Set alerts and permissions

### API / CLI Method

```bash
# Create job via CLI
databricks jobs create --json '{
  "name": "Daily ETL",
  "tasks": [...],
  "schedule": {...}
}'

# List jobs
databricks jobs list

# Run job
databricks jobs run-now --job-id 12345
```

### Terraform / Infrastructure as Code

```hcl
resource "databricks_job" "etl_job" {
  name = "Daily ETL Pipeline"

  task {
    task_key = "ingest"
    notebook_task {
      notebook_path = "/Repos/prod/notebooks/ingest"
    }
    new_cluster {
      num_workers   = 2
      spark_version = "13.3.x-scala2.12"
      node_type_id  = "i3.xlarge"
    }
  }

  task {
    task_key = "transform"
    depends_on {
      task_key = "ingest"
    }
    notebook_task {
      notebook_path = "/Repos/prod/notebooks/transform"
    }
  }

  schedule {
    quartz_cron_expression = "0 0 6 * * ?"
    timezone_id            = "UTC"
  }
}
```

## Task Dependencies

### Linear Dependencies

```
Task A → Task B → Task C
```

```json
{
  "tasks": [
    {"task_key": "A"},
    {"task_key": "B", "depends_on": [{"task_key": "A"}]},
    {"task_key": "C", "depends_on": [{"task_key": "B"}]}
  ]
}
```

### Parallel Tasks

```
        ┌─→ Task B ─┐
Task A ─┤           ├─→ Task D
        └─→ Task C ─┘
```

```json
{
  "tasks": [
    {"task_key": "A"},
    {"task_key": "B", "depends_on": [{"task_key": "A"}]},
    {"task_key": "C", "depends_on": [{"task_key": "A"}]},
    {"task_key": "D", "depends_on": [{"task_key": "B"}, {"task_key": "C"}]}
  ]
}
```

### Conditional (If/Else)

```python
# In notebook, set task value
dbutils.jobs.taskValues.set(key="status", value="success")

# If/Else task checks condition
# Condition: {{tasks.check_task.values.status}} == "success"
```

### For Each (Loops)

```json
{
  "task_key": "process_files",
  "for_each_task": {
    "inputs": "[\"file1.csv\", \"file2.csv\", \"file3.csv\"]",
    "task": {
      "task_key": "process_single",
      "notebook_task": {
        "notebook_path": "/process",
        "base_parameters": {
          "file": "{{input}}"
        }
      }
    }
  }
}
```

## Cluster Configuration

### Job Clusters (Recommended)

Created for the job, terminated after:

```json
{
  "new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 4,
    "spark_conf": {
      "spark.sql.shuffle.partitions": "100"
    }
  }
}
```

### All-Purpose Clusters

Shared, long-running cluster:

```json
{
  "existing_cluster_id": "1234-567890-abcdef"
}
```

### Serverless Compute

No cluster management:

```json
{
  "environment_key": "default"
}
```

### Cluster Sharing

Multiple tasks share one cluster:

```json
{
  "job_clusters": [
    {
      "job_cluster_key": "shared_cluster",
      "new_cluster": {...}
    }
  ],
  "tasks": [
    {"task_key": "A", "job_cluster_key": "shared_cluster"},
    {"task_key": "B", "job_cluster_key": "shared_cluster"}
  ]
}
```

## Scheduling

### Cron Expressions

```
┌───────────── second (0-59)
│ ┌───────────── minute (0-59)
│ │ ┌───────────── hour (0-23)
│ │ │ ┌───────────── day of month (1-31)
│ │ │ │ ┌───────────── month (1-12)
│ │ │ │ │ ┌───────────── day of week (0-6, SUN-SAT)
│ │ │ │ │ │
* * * * * *
```

| Schedule | Cron Expression |
|----------|-----------------|
| Daily at 6 AM | `0 0 6 * * ?` |
| Hourly | `0 0 * * * ?` |
| Every 15 minutes | `0 */15 * * * ?` |
| Weekdays at 9 AM | `0 0 9 ? * MON-FRI` |
| First of month | `0 0 0 1 * ?` |

### Continuous Jobs

Run immediately when previous completes:

```json
{
  "continuous": {
    "pause_status": "UNPAUSED"
  }
}
```

### Trigger-Based

File arrival trigger:

```json
{
  "trigger": {
    "file_arrival": {
      "url": "s3://bucket/path/",
      "min_time_between_triggers_seconds": 60
    }
  }
}
```

## Parameters

### Job Parameters

```json
{
  "parameters": [
    {"name": "date", "default": "{{job.start_time.iso_date}}"},
    {"name": "env", "default": "prod"}
  ]
}
```

### Task Parameters

```json
{
  "notebook_task": {
    "notebook_path": "/my_notebook",
    "base_parameters": {
      "input_path": "/data/raw/",
      "output_path": "/data/processed/"
    }
  }
}
```

### Dynamic Parameters

| Variable | Description |
|----------|-------------|
| `{{job.id}}` | Job ID |
| `{{job.run_id}}` | Current run ID |
| `{{job.start_time.iso_date}}` | Run start date (YYYY-MM-DD) |
| `{{tasks.task_key.values.key}}` | Task value from previous task |

### Accessing Parameters in Notebook

```python
# Get widget parameters
date = dbutils.widgets.get("date")
env = dbutils.widgets.get("env")

# Or from job context
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
run_id = context.currentRunId().get()
```

## Task Values (Passing Data Between Tasks)

### Set Value

```python
# In Task A
result = process_data()
dbutils.jobs.taskValues.set(key="record_count", value=result["count"])
dbutils.jobs.taskValues.set(key="status", value="success")
```

### Get Value

```python
# In Task B (depends on Task A)
count = dbutils.jobs.taskValues.get(taskKey="task_a", key="record_count")
print(f"Task A processed {count} records")
```

### Use in Conditions

```
# If/Else condition
{{tasks.task_a.values.status}} == "success"
```

## Error Handling

### Retry Policy

```json
{
  "task_key": "my_task",
  "retry_on_timeout": true,
  "max_retries": 3,
  "min_retry_interval_millis": 60000,
  "notebook_task": {...}
}
```

### Timeout

```json
{
  "timeout_seconds": 3600  // 1 hour
}
```

### Alerts

```json
{
  "email_notifications": {
    "on_start": ["team@company.com"],
    "on_success": ["team@company.com"],
    "on_failure": ["oncall@company.com"],
    "no_alert_for_skipped_runs": true
  },
  "webhook_notifications": {
    "on_failure": [{"id": "slack-webhook-id"}]
  }
}
```

### Run If

Control when tasks run:

| Condition | Behavior |
|-----------|----------|
| `ALL_SUCCESS` | Run if all dependencies succeeded (default) |
| `AT_LEAST_ONE_SUCCESS` | Run if any dependency succeeded |
| `NONE_FAILED` | Run if no dependency failed |
| `ALL_DONE` | Run when all dependencies complete (success or fail) |
| `AT_LEAST_ONE_FAILED` | Run if any dependency failed |
| `ALL_FAILED` | Run if all dependencies failed |

```json
{
  "task_key": "cleanup",
  "depends_on": [{"task_key": "main"}],
  "run_if": "ALL_DONE"  // Always run cleanup
}
```

## Monitoring

### Job Runs UI

View in: **Workflows → Job → Runs**

Shows:
- Run history
- Task status
- Duration
- Logs and output

### System Tables

```sql
-- Query job run history
SELECT * FROM system.lakeflow.job_runs
WHERE job_id = 12345
ORDER BY start_time DESC;

-- Query task run history
SELECT * FROM system.lakeflow.task_runs
WHERE job_id = 12345;
```

### Metrics API

```python
# Get job run status
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
run = w.jobs.get_run(run_id=12345)
print(f"Status: {run.state.life_cycle_state}")
```

## Best Practices

### 1. Use Job Clusters

```json
// ✅ Good: Job cluster (auto-terminates)
"new_cluster": {...}

// ❌ Avoid: All-purpose cluster (keeps running)
"existing_cluster_id": "..."
```

### 2. Modular Tasks

```
// ✅ Good: Separate tasks
Task 1: Ingest → Task 2: Transform → Task 3: Load

// ❌ Avoid: Monolithic notebook
Task 1: Everything
```

### 3. Idempotent Operations

```python
# ✅ Good: Overwrite mode (re-runnable)
df.write.mode("overwrite").saveAsTable("target")

# ❌ Avoid: Append without dedup (duplicates on retry)
df.write.mode("append").saveAsTable("target")
```

### 4. Use Repos for Version Control

```json
{
  "git_source": {
    "url": "https://github.com/org/repo",
    "provider": "github",
    "branch": "main"
  },
  "notebook_task": {
    "notebook_path": "notebooks/etl"
  }
}
```

### 5. Environment-Specific Parameters

```json
{
  "parameters": [
    {"name": "env", "default": "dev"},
    {"name": "catalog", "default": "{{env}}_catalog"}
  ]
}
```

## Exam Questions

### Q1: Job vs All-Purpose Cluster
**Why prefer job clusters over all-purpose clusters for workflows?**

Job clusters auto-terminate after the job, saving costs. All-purpose clusters keep running.

### Q2: Task Dependencies
**How do you make Task B run only after Task A succeeds?**

```json
{"task_key": "B", "depends_on": [{"task_key": "A"}]}
```

### Q3: Passing Data
**How do you pass data between tasks?**

Use `dbutils.jobs.taskValues.set()` and `dbutils.jobs.taskValues.get()`

### Q4: Error Handling
**How do you ensure a cleanup task always runs?**

Set `"run_if": "ALL_DONE"` on the cleanup task.

### Q5: Scheduling
**What cron expression runs a job daily at 6 AM UTC?**

`0 0 6 * * ?`

### Q6: Triggers
**How do you trigger a job when new files arrive?**

Use file arrival trigger:
```json
{"trigger": {"file_arrival": {"url": "s3://bucket/path/"}}}
```

### Q7: Retries
**How do you configure automatic retries on failure?**

Set `"max_retries": 3` and `"retry_on_timeout": true`

## CLI Commands

```bash
# List jobs
databricks jobs list

# Get job details
databricks jobs get --job-id 12345

# Run job now
databricks jobs run-now --job-id 12345

# Run with parameters
databricks jobs run-now --job-id 12345 --params '{"date": "2024-01-01"}'

# Cancel run
databricks runs cancel --run-id 67890

# Get run output
databricks runs get-output --run-id 67890
```
