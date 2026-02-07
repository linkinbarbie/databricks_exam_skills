# Practice Exam Questions

## Unity Catalog

### Q1: Hierarchy
**What is the correct hierarchy in Unity Catalog?**

A) Metastore → Schema → Catalog → Table
B) Catalog → Metastore → Schema → Table
C) Metastore → Catalog → Schema → Table ✅
D) Schema → Catalog → Metastore → Table

---

### Q2: Three-Level Namespace
**Which is the correct way to reference a table in Unity Catalog?**

A) `schema.table`
B) `catalog.table`
C) `metastore.catalog.schema.table`
D) `catalog.schema.table` ✅

---

### Q3: Managed vs External
**What happens when you DROP a managed table in Unity Catalog?**

A) Only metadata is deleted
B) Only data files are deleted
C) Both metadata and data files are deleted ✅
D) Neither is deleted

---

### Q4: Permissions
**A user has SELECT on a table but cannot access it. What is likely missing?**

A) MODIFY permission
B) USE CATALOG and/or USE SCHEMA permissions ✅
C) OWNERSHIP permission
D) CREATE TABLE permission

---

### Q5: Storage Credential
**What does a storage credential provide?**

A) Access to compute resources
B) Authentication to external cloud storage ✅
C) Network connectivity
D) Query caching

---

### Q6: External Location
**What must you create before creating an external table at an S3 location?**

A) S3 bucket only
B) Storage credential and external location ✅
C) IAM policy only
D) VPC endpoint

---

## Delta Lake

### Q7: Time Travel
**Which syntax correctly queries a Delta table at version 5?**

A) `SELECT * FROM table AT VERSION 5`
B) `SELECT * FROM table VERSION AS OF 5` ✅
C) `SELECT * FROM table@v5`
D) `SELECT * FROM table HISTORY 5`

---

### Q8: VACUUM
**What is the minimum retention period for VACUUM in production?**

A) 0 hours
B) 24 hours
C) 168 hours (7 days) ✅
D) 720 hours (30 days)

---

### Q9: OPTIMIZE
**What does OPTIMIZE do?**

A) Deletes old files
B) Compacts small files into larger files ✅
C) Creates statistics
D) Repairs corrupted data

---

### Q10: Z-ORDER
**When should you use Z-ORDER?**

A) When you have many small files
B) When queries frequently filter on specific columns ✅
C) When you need to delete data
D) When you need faster writes

---

### Q11: Schema Evolution
**How do you add a column to a Delta table?**

A) `ADD COLUMN TO table column_name`
B) `ALTER TABLE table ADD COLUMN column_name type` ✅
C) `MODIFY TABLE table ADD column_name`
D) `UPDATE SCHEMA table ADD column_name`

---

### Q12: MERGE
**Which statement correctly performs an upsert in Delta Lake?**

A) `INSERT OR UPDATE INTO target ...`
B) `UPSERT INTO target ...`
C) `MERGE INTO target USING source ON condition WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT` ✅
D) `UPDATE OR INSERT target ...`

---

## SQL Warehouse

### Q13: Serverless
**What is a benefit of Serverless SQL Warehouse?**

A) Lower per-hour cost
B) Instant startup and auto-scaling ✅
C) Support for Python
D) Unlimited concurrency

---

### Q14: Use Case
**Which workload is best suited for SQL Warehouse?**

A) Training ML models
B) Running Spark notebooks
C) BI dashboards and SQL queries ✅
D) Streaming ETL

---

## Governance

### Q15: Row-Level Security
**How do you implement row-level security in Unity Catalog?**

A) Create a view with WHERE clause
B) Apply a row filter function to the table ✅
C) Use column masking
D) Create separate tables per user

---

### Q16: Column Masking
**What does column masking do?**

A) Hides entire columns from all users
B) Returns masked values based on user permissions ✅
C) Encrypts column data at rest
D) Removes columns from schema

---

### Q17: Data Lineage
**Unity Catalog automatically tracks lineage for:**

A) Only tables created via SQL
B) Only Delta tables
C) Tables and columns across queries ✅
D) Only external tables

---

### Q18: Audit Logs
**Where can you find Unity Catalog audit logs?**

A) CloudWatch only
B) system.access.audit table ✅
C) _delta_log directory
D) Spark driver logs

---

## Answers Key

1. C
2. D
3. C
4. B
5. B
6. B
7. B
8. C
9. B
10. B
11. B
12. C
13. B
14. C
15. B
16. B
17. C
18. B

## Exam Tips

1. **Know the hierarchy**: Metastore → Catalog → Schema → Object
2. **Permissions cascade**: Need USE on parents to access children
3. **Managed vs External**: Managed = UC controls storage, External = you control
4. **VACUUM default**: 7 days (168 hours) minimum
5. **OPTIMIZE vs VACUUM**: Optimize compacts, Vacuum deletes old files
6. **Time travel**: Needs files to exist (not vacuumed)
7. **Z-ORDER**: For filtered query columns
8. **SQL Warehouse**: BI/SQL only, cheaper for queries
9. **Row filter vs Column mask**: Row = filter rows, Column = mask values
