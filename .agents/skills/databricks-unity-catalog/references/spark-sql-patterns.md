# Spark SQL Patterns for Certification

## Essential SQL Operations

### SELECT and Filtering

```sql
-- Basic SELECT
SELECT column1, column2 FROM table;

-- Aliases
SELECT
    customer_id AS id,
    first_name || ' ' || last_name AS full_name,
    order_amount * 1.1 AS amount_with_tax
FROM orders;

-- DISTINCT
SELECT DISTINCT category FROM products;

-- WHERE clause
SELECT * FROM orders
WHERE status = 'completed'
  AND amount > 100
  AND order_date >= '2024-01-01';

-- IN, NOT IN
SELECT * FROM orders WHERE status IN ('pending', 'processing');
SELECT * FROM orders WHERE customer_id NOT IN (SELECT id FROM blocked_customers);

-- BETWEEN
SELECT * FROM orders WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31';

-- LIKE patterns
SELECT * FROM customers WHERE email LIKE '%@gmail.com';
SELECT * FROM products WHERE name LIKE 'iPhone%';

-- NULL handling
SELECT * FROM orders WHERE shipped_date IS NULL;
SELECT * FROM orders WHERE shipped_date IS NOT NULL;
SELECT COALESCE(discount, 0) AS discount FROM orders;  -- Replace NULL with 0
SELECT NULLIF(amount, 0) AS amount FROM orders;  -- Return NULL if equals 0
```

### JOINs

```sql
-- INNER JOIN (matching rows only)
SELECT o.order_id, c.customer_name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id;

-- LEFT JOIN (all from left, matching from right)
SELECT o.order_id, c.customer_name
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id;

-- RIGHT JOIN (all from right, matching from left)
SELECT o.order_id, c.customer_name
FROM orders o
RIGHT JOIN customers c ON o.customer_id = c.id;

-- FULL OUTER JOIN (all from both)
SELECT o.order_id, c.customer_name
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.id;

-- CROSS JOIN (cartesian product)
SELECT * FROM sizes CROSS JOIN colors;

-- Self JOIN
SELECT e.name AS employee, m.name AS manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id;

-- Multiple JOINs
SELECT
    o.order_id,
    c.customer_name,
    p.product_name,
    oi.quantity
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.id;
```

### Aggregations

```sql
-- Basic aggregations
SELECT
    COUNT(*) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value,
    MIN(amount) AS min_order,
    MAX(amount) AS max_order
FROM orders;

-- GROUP BY
SELECT
    customer_id,
    COUNT(*) AS order_count,
    SUM(amount) AS total_spent
FROM orders
GROUP BY customer_id;

-- Multiple GROUP BY columns
SELECT
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    category,
    SUM(amount) AS revenue
FROM orders
GROUP BY YEAR(order_date), MONTH(order_date), category;

-- HAVING (filter after aggregation)
SELECT
    customer_id,
    SUM(amount) AS total_spent
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > 1000;

-- Aggregation with CASE
SELECT
    COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending,
    COUNT(CASE WHEN status = 'cancelled' THEN 1 END) AS cancelled
FROM orders;
```

### Window Functions (CRITICAL FOR EXAM)

```sql
-- ROW_NUMBER: Unique sequential number
SELECT
    customer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_num
FROM orders;

-- RANK: Same rank for ties, gaps after
SELECT
    product_id,
    revenue,
    RANK() OVER (ORDER BY revenue DESC) AS rank
FROM product_sales;
-- Result: 1, 2, 2, 4 (gap after tie)

-- DENSE_RANK: Same rank for ties, no gaps
SELECT
    product_id,
    revenue,
    DENSE_RANK() OVER (ORDER BY revenue DESC) AS dense_rank
FROM product_sales;
-- Result: 1, 2, 2, 3 (no gap)

-- LAG: Previous row value
SELECT
    order_date,
    amount,
    LAG(amount, 1) OVER (ORDER BY order_date) AS prev_amount,
    amount - LAG(amount, 1) OVER (ORDER BY order_date) AS change
FROM daily_sales;

-- LEAD: Next row value
SELECT
    order_date,
    amount,
    LEAD(amount, 1) OVER (ORDER BY order_date) AS next_amount
FROM daily_sales;

-- Running totals
SELECT
    order_date,
    amount,
    SUM(amount) OVER (ORDER BY order_date) AS running_total
FROM orders;

-- Running total within partition
SELECT
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) AS customer_running_total
FROM orders;

-- Moving average
SELECT
    order_date,
    amount,
    AVG(amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS seven_day_avg
FROM daily_sales;

-- First/Last value
SELECT
    customer_id,
    order_date,
    amount,
    FIRST_VALUE(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) AS first_order_amount,
    LAST_VALUE(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_order_amount
FROM orders;

-- Percentile
SELECT
    product_id,
    revenue,
    NTILE(4) OVER (ORDER BY revenue) AS quartile,
    PERCENT_RANK() OVER (ORDER BY revenue) AS percentile
FROM product_sales;
```

### Common Table Expressions (CTEs)

```sql
-- Simple CTE
WITH high_value_customers AS (
    SELECT customer_id, SUM(amount) AS total_spent
    FROM orders
    GROUP BY customer_id
    HAVING SUM(amount) > 10000
)
SELECT c.*, hvc.total_spent
FROM customers c
JOIN high_value_customers hvc ON c.id = hvc.customer_id;

-- Multiple CTEs
WITH
monthly_sales AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        SUM(amount) AS revenue
    FROM orders
    GROUP BY DATE_TRUNC('month', order_date)
),
monthly_growth AS (
    SELECT
        month,
        revenue,
        LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
        (revenue - LAG(revenue) OVER (ORDER BY month)) / LAG(revenue) OVER (ORDER BY month) * 100 AS growth_pct
    FROM monthly_sales
)
SELECT * FROM monthly_growth WHERE growth_pct > 10;

-- Recursive CTE (hierarchies)
WITH RECURSIVE org_hierarchy AS (
    -- Base case: top-level managers
    SELECT id, name, manager_id, 1 AS level
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive case: employees with managers
    SELECT e.id, e.name, e.manager_id, h.level + 1
    FROM employees e
    JOIN org_hierarchy h ON e.manager_id = h.id
)
SELECT * FROM org_hierarchy ORDER BY level, name;
```

### Subqueries

```sql
-- Scalar subquery
SELECT
    order_id,
    amount,
    amount - (SELECT AVG(amount) FROM orders) AS diff_from_avg
FROM orders;

-- IN subquery
SELECT * FROM customers
WHERE id IN (SELECT DISTINCT customer_id FROM orders WHERE amount > 1000);

-- EXISTS subquery
SELECT * FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.id AND o.amount > 1000
);

-- Correlated subquery
SELECT
    o1.customer_id,
    o1.order_date,
    o1.amount,
    (SELECT COUNT(*) FROM orders o2
     WHERE o2.customer_id = o1.customer_id
     AND o2.order_date < o1.order_date) AS previous_orders
FROM orders o1;
```

### Set Operations

```sql
-- UNION (distinct rows)
SELECT customer_id FROM orders_2023
UNION
SELECT customer_id FROM orders_2024;

-- UNION ALL (all rows, including duplicates)
SELECT customer_id FROM orders_2023
UNION ALL
SELECT customer_id FROM orders_2024;

-- INTERSECT (rows in both)
SELECT customer_id FROM orders_2023
INTERSECT
SELECT customer_id FROM orders_2024;

-- EXCEPT / MINUS (rows in first but not second)
SELECT customer_id FROM orders_2023
EXCEPT
SELECT customer_id FROM orders_2024;
```

### PIVOT and UNPIVOT

```sql
-- PIVOT (rows to columns)
SELECT * FROM (
    SELECT category, MONTH(order_date) AS month, amount
    FROM orders
)
PIVOT (
    SUM(amount) FOR month IN (1 AS Jan, 2 AS Feb, 3 AS Mar)
);

-- UNPIVOT (columns to rows)
SELECT * FROM sales_wide
UNPIVOT (
    revenue FOR month IN (jan_revenue, feb_revenue, mar_revenue)
);
```

### Date Functions

```sql
-- Current date/time
SELECT
    current_date(),
    current_timestamp(),
    now();

-- Date extraction
SELECT
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    DAY(order_date) AS day,
    DAYOFWEEK(order_date) AS dow,
    WEEKOFYEAR(order_date) AS week,
    QUARTER(order_date) AS quarter
FROM orders;

-- Date arithmetic
SELECT
    DATE_ADD(order_date, 7) AS plus_7_days,
    DATE_SUB(order_date, 30) AS minus_30_days,
    DATEDIFF(shipped_date, order_date) AS days_to_ship,
    MONTHS_BETWEEN(end_date, start_date) AS months_diff
FROM orders;

-- Date truncation
SELECT
    DATE_TRUNC('month', order_date) AS month_start,
    DATE_TRUNC('year', order_date) AS year_start,
    DATE_TRUNC('week', order_date) AS week_start
FROM orders;

-- Date formatting
SELECT
    DATE_FORMAT(order_date, 'yyyy-MM-dd') AS iso_date,
    DATE_FORMAT(order_date, 'MM/dd/yyyy') AS us_date,
    DATE_FORMAT(order_date, 'EEEE, MMMM d, yyyy') AS long_date
FROM orders;

-- String to date
SELECT
    TO_DATE('2024-01-15', 'yyyy-MM-dd') AS parsed_date,
    TO_TIMESTAMP('2024-01-15 10:30:00', 'yyyy-MM-dd HH:mm:ss') AS parsed_ts
FROM orders;
```

### String Functions

```sql
SELECT
    -- Case
    UPPER(name) AS upper_name,
    LOWER(name) AS lower_name,
    INITCAP(name) AS title_case,

    -- Trimming
    TRIM(name) AS trimmed,
    LTRIM(name) AS left_trimmed,
    RTRIM(name) AS right_trimmed,

    -- Substring
    SUBSTRING(phone, 1, 3) AS area_code,
    LEFT(name, 1) AS first_initial,
    RIGHT(phone, 4) AS last_four,

    -- Position/Search
    INSTR(email, '@') AS at_position,
    LOCATE('@', email) AS at_position2,

    -- Replace
    REPLACE(phone, '-', '') AS phone_no_dash,
    REGEXP_REPLACE(phone, '[^0-9]', '') AS digits_only,

    -- Concatenation
    CONCAT(first_name, ' ', last_name) AS full_name,
    first_name || ' ' || last_name AS full_name2,

    -- Length
    LENGTH(name) AS name_length,

    -- Padding
    LPAD(id, 10, '0') AS padded_id,
    RPAD(name, 20, ' ') AS right_padded,

    -- Split
    SPLIT(tags, ',') AS tag_array,
    SPLIT(tags, ',')[0] AS first_tag
FROM customers;
```

### CASE Expressions

```sql
-- Simple CASE
SELECT
    order_id,
    CASE status
        WHEN 'P' THEN 'Pending'
        WHEN 'C' THEN 'Completed'
        WHEN 'X' THEN 'Cancelled'
        ELSE 'Unknown'
    END AS status_name
FROM orders;

-- Searched CASE
SELECT
    order_id,
    amount,
    CASE
        WHEN amount >= 1000 THEN 'High'
        WHEN amount >= 100 THEN 'Medium'
        ELSE 'Low'
    END AS order_tier
FROM orders;

-- CASE in aggregation
SELECT
    customer_id,
    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_revenue,
    SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) AS pending_revenue
FROM orders
GROUP BY customer_id;

-- CASE in ORDER BY
SELECT * FROM orders
ORDER BY
    CASE status
        WHEN 'urgent' THEN 1
        WHEN 'high' THEN 2
        WHEN 'normal' THEN 3
        ELSE 4
    END;
```

### Higher-Order Functions (Arrays)

```sql
-- Transform array elements
SELECT TRANSFORM(ARRAY(1, 2, 3), x -> x * 2);  -- [2, 4, 6]

-- Filter array elements
SELECT FILTER(ARRAY(1, 2, 3, 4, 5), x -> x > 2);  -- [3, 4, 5]

-- Check if any/all match
SELECT EXISTS(ARRAY(1, 2, 3), x -> x > 2);  -- true
SELECT FORALL(ARRAY(1, 2, 3), x -> x > 0);  -- true

-- Reduce/aggregate array
SELECT REDUCE(ARRAY(1, 2, 3, 4), 0, (acc, x) -> acc + x);  -- 10

-- Array functions
SELECT
    ARRAY_CONTAINS(tags, 'sale') AS has_sale_tag,
    ARRAY_SIZE(tags) AS tag_count,
    ARRAY_DISTINCT(tags) AS unique_tags,
    ARRAY_UNION(tags1, tags2) AS combined_tags,
    ARRAY_INTERSECT(tags1, tags2) AS common_tags,
    EXPLODE(tags) AS individual_tag  -- One row per element
FROM products;
```

### JSON Functions

```sql
-- Extract from JSON
SELECT
    GET_JSON_OBJECT(json_col, '$.name') AS name,
    GET_JSON_OBJECT(json_col, '$.address.city') AS city,
    json_col:name AS name2,  -- Dot notation
    json_col:address.city AS city2
FROM events;

-- Parse JSON
SELECT FROM_JSON(json_string, 'name STRING, age INT') AS parsed;

-- Create JSON
SELECT TO_JSON(STRUCT(name, age)) AS json_output FROM users;

-- Schema of JSON
SELECT SCHEMA_OF_JSON('[{"name": "John", "age": 30}]');
```

## Exam Tips

1. **Window functions are heavily tested** - Know ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD
2. **Know the difference** between RANK and DENSE_RANK
3. **CTEs** are preferred over nested subqueries
4. **COALESCE** and **NULLIF** for NULL handling
5. **DATE_TRUNC** for time-based aggregations
6. **CASE expressions** for conditional logic
