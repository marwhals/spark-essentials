# Spark SQL

---

## Objective

- Use Spark as a "big data" database

---

## SQL

- Universal standard for accessing structured data
- Abstraction over DataFrames for engineers familiar with databases
- Supported in Spark
  - Programmatically in expressions
  - In the Spark shell
- Spark SQL
  - Has the concept of "database", "table", "view"
  - Allows accessing DataFrames as tables
- DataFrame vs table
  - identical in terms of storage and partitioning
  - DataFrames can be processed programmatically, table in SQL

---
# Spark Tables

- Managed
  - Spark is in charge of the metadata + the data.
  - If you drop the table, you lose the data‼.
- External (unmanaged)
  - Spark is in charge of the metadata only.
  - If you drop the table, you keep the data.

---

[//]: # (TODO: see spark cluster folder to spin up practice environment)