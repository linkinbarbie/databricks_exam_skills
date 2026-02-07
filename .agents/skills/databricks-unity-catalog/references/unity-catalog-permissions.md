# Unity Catalog Permissions Deep Dive

## Permission Types

| Permission | Applies To | Description |
|------------|-----------|-------------|
| `USE CATALOG` | Catalog | Access catalog, required to see schemas |
| `USE SCHEMA` | Schema | Access schema, required to see tables |
| `SELECT` | Table/View | Read data |
| `MODIFY` | Table | INSERT, UPDATE, DELETE, MERGE |
| `CREATE TABLE` | Schema | Create tables in schema |
| `CREATE SCHEMA` | Catalog | Create schemas in catalog |
| `CREATE CATALOG` | Metastore | Create catalogs |
| `ALL PRIVILEGES` | Any | All applicable permissions |
| `OWNERSHIP` | Any | Full control including GRANT/DROP |

## Permission Inheritance

```
Metastore
    └── Catalog (USE CATALOG required)
            └── Schema (USE SCHEMA required)
                    └── Table (SELECT/MODIFY)
```

**Key Rule**: You must have USE permissions on parents to access children.

```sql
-- This fails if user doesn't have USE CATALOG on prod
GRANT SELECT ON TABLE prod.sales.orders TO `analyst`;

-- Correct: Grant parent permissions first
GRANT USE CATALOG ON CATALOG prod TO `analyst`;
GRANT USE SCHEMA ON SCHEMA prod.sales TO `analyst`;
GRANT SELECT ON TABLE prod.sales.orders TO `analyst`;
```

## Groups vs Users

```sql
-- Best practice: Grant to groups, not users
GRANT USE CATALOG ON CATALOG prod TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA prod.raw TO `data-engineers`;

-- Add users to groups via account console or SCIM
```

## Ownership

```sql
-- Check owner
DESCRIBE CATALOG EXTENDED prod;

-- Transfer ownership
ALTER CATALOG prod OWNER TO `data-platform-team`;
ALTER SCHEMA prod.sales OWNER TO `sales-team`;
ALTER TABLE prod.sales.orders OWNER TO `sales-admins`;
```

**Owner can:**
- Grant/revoke permissions to others
- Drop the object
- Transfer ownership

## Common Permission Patterns

### Read-Only Analyst
```sql
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA ON SCHEMA prod.curated TO `analysts`;
GRANT SELECT ON SCHEMA prod.curated TO `analysts`;  -- All tables in schema
```

### Data Engineer (Read/Write)
```sql
GRANT USE CATALOG ON CATALOG prod TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA prod.raw TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA prod.raw TO `data-engineers`;
```

### Schema Admin
```sql
GRANT USE CATALOG ON CATALOG prod TO `schema-admin`;
GRANT CREATE SCHEMA ON CATALOG prod TO `schema-admin`;
GRANT USE SCHEMA ON SCHEMA prod.* TO `schema-admin`;
GRANT ALL PRIVILEGES ON SCHEMA prod.* TO `schema-admin`;
```

## Audit Permissions

```sql
-- Check who has access to a table
SHOW GRANTS ON TABLE prod.sales.orders;

-- Check what a principal has access to
SHOW GRANTS TO `data-team`;

-- Query system tables for audit
SELECT * FROM system.access.audit
WHERE action_name = 'getTable'
ORDER BY event_time DESC;
```

## Troubleshooting

**"Permission denied"**
1. Check USE CATALOG permission
2. Check USE SCHEMA permission
3. Check object-level permission (SELECT/MODIFY)

```sql
-- Debug permissions
SHOW GRANTS TO `current_user()`;
SHOW GRANTS ON CATALOG prod;
SHOW GRANTS ON SCHEMA prod.sales;
SHOW GRANTS ON TABLE prod.sales.orders;
```
