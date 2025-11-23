
## 3. Databricks

Databricks is a commercial solution called a lakehouse (combination of data lake and data warehouse). Mostly known for:

- separating storage and compute
- storage:
	- data lake:
		- can easily store raw data (any kind of file)
		- uses cloud object/blob storage, so it is cheap even for big data
	- data warehouse:
		- holds structured data (primarily in delta tables)
- compute:
	- queries are possible in SQL and Python (lazy DataFrame)
	- Databricks prepares clusters for you
	- there is an option for serverless compute (can be tricky to manage costs)

Jobs (workflows): automate data pipelines.
Lakeflow connect: integrates with various data sources (files, cloud storage, Kafka, etc.).
Supports batch, incremental, and streaming modes.

Your data are organized in **workspace**. Main entities in workspace are:
- **notebooks**: interactive code cells
- **jobs**: workflows, there are divided into **tasks**.
- **tables**: structured data

### Unity catalog

Centralized data governance and access control.

Roles:

- (catalog) admin
- admin

### Git

Version control for notebooks and code.

Databricks assets bundles

### Ingestion

Basic info:

```SQL
SELECT current_catalog(), current_schema(); -- catalog/schema info
LIST <path>;                    -- list files
SELECT * FROM parquet.`<path>`; -- read Parquet file directly
```

#### Batch

Tradiotinaly CTAS (`CREATE TABLE AS`, `spark.read.load()`) aproach is used for ingestion. It reads all rows (entire data) each time. It is not efficient for large data that changes slowly.

```SQL
CREATE TABLE new_table AS
SELECT *
FROM read_files(
    <path_to_files>,
    format => '<file_type>',    -- JSON, CSV, XML, TEXT, BINARYFILE, PARQUET, AVRO
    <other_format_specific_options>
);
```

#### Incremental batch

Only new data are ingested. For this is used:

- `COPY INTO`
- spark.readStream (Auto loader with time trigger)
- Declarative pipeline: `CREATE OR REFRESH STREAMING TABLE`

```SQL
CREATE TABLE new_table_2;   -- Create empty table

COPY INTO new_table_2
    FROM '<dir_path>'
    FILEFORMAT = <file_type>
    FORMAT_OPTIONS(<options>)
    COPY_OPTIONS(<options>)

--- better approach: ---

CREATE OR REFRESH STREAMING TABLE new_table_3
SCHEDULE EVERY 1 WEEK
AS SELECT *
FROM STREAM read_files(...);-- e.g., 'mergeSchema' = 'true'
```

Automatically skip already ingested files based on checkpointing. Operation is idempotent.

Autoloader is more modern than COPY INTO.

#### Streaming

Continously ingests new data as it arrives. Suitable for real-time data processing.

- `spark.readStream` (autoloader with continous trigger)
- Declarative pipeline trigger mode continous

Use cases: sensor data, logs, real-time analytics, sources: Kafka, event hubs, etc.

Streaming table: The easiest way is to keep adding new files to the storage. This table periodically updates itself automatically with new data. This table is useful for ever-growing data, like data from sensors. It is efficient because it just appends new parquet files containing new data.

```SQL
CREATE OR REFRESH STREAMING TABLE  my_table_name
SCHEDULE EVERY 1 WEEK             -- refresh/update period
AS
SELECT *
FROM STREAM read_files(
  '/volumes/project/my_streaming' -- path to your files
  format => 'CSV',
  sep => '|',
  header => true
);
```

```python
spark
.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", "<checkpoint_path>")
  .load("/volumes/project/my_streaming")
.writeStream
  .option("checkpointLocation", "<checkpoint_path>")
  .trigger(processingTime="5 seconds")
  .toTable("my_table_name"
```

Force refresh: `REFRESH STREAMING TABLE my_table_name`


##### Metadata and schema mismatches

During ingestion you can apply various metadata:
`_metadata.file_modification_time`
`_metadata.file_name`

And handle schema mismatches:
`_rescued_data`


---

Ingestion into existing table: `MERGE INTO` (upsert), various strategy. God for slowly chaging dimensions (SCDs), incremental loads, and complex change data capture (CDC).


#### Comparasion

![tables](./img/tables.png)

Managed tables: Databricks-managed storage.
External tables: data stored outside Databricks (e.g., cloud storage).

#### JSON

There are 3 options how to handle JSON in column:

1. Store as a string, it is easy but not efficient.
    Query with path syntax: `SELECT json_col:address:city FROM table`
2. Use `Struct` type, better for fixed schema.
    Derive schema: `SELECT schema_of_json('sample-json-string')`
    Convert JSON to struct: `SELECT from_json(json_col, 'json-struct-schema') AS struct_column FROM table`
3. Use `Variant` type, it combinate advantages of both.
    Store any JSON structure, flexible schema.
    Parse: `parse_json( jsonStr )`
    Query with path syntax: `SELECT variant_col:address:city FROM table`

### Datatypes

**STREAMING TABLE:**

- support fot batch or streaming for incremental data processing.
- each time an streaming table is refreshed, it processes only new data since the last refresh.
- create SQL: `CREATE OR REFRESH STREAMING TABLE`
- In SQL you must to use `FROM STREAM read_files()` to invoke Auto Loader functionality for incremental streaming reads and checkpoints

**MATERIALIZED VIEW (MV):**

- Each time a materialized view is updated, query results are recalculated to reflect changes in upstream datasets
- Created and kept up-to-date by pipeline
- Use the `CREATE OR REFRESH MATERIALIZED VIEW` syntax
- Can be used anywhere in your pipeline, not just in the gold layer
- Where applicable, results are incrementally refreshed for materialized views, avoiding the need to completely rebuild the materialized view when new data arrives (Serverkess Only)
- Incremental refresh for materialized views is a cost-based optimizer, to power fast and efficient transformations for materialized views on Serveless compute

**VIEWS:**

TEMPORARY VIEW:

- temporary views are only persisted acriss the lifetime of the pipeline and private to the defining pipeline
- they are not registered as an object to Unity Catalog
- Use the `CREATE TEMPORARY VIEW` statement
- TM are useful as intermediate queries that are not exposed to end users

VIEW:

- no physial data
- they are registered as an object to Unity Catalog
- Use the `CREATE VIEW` statement

Limitations of views:

- the pipeline must be a Unity Catalog pipeline
- views cannot have streaming queries, or be used as a streaming source

CONSTRAINTS:

Levels:
WARN  default, row is written but a warning is logged
DROP  row is dropped
FAIL  entire transaction fails

```SQL
CREATE OR REFRESH STREAMING TABLE silver_db.orders_silver
(
CONSTRAINT valid_notify EXPECT (notifications IN ('Y', 'N')),
CONSTRAINT valid_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01" ON VIOLATION FAIL UPDATE
)
AS SELECT ...
```

**JOINS**

streaming table -> joined streaming table <- static table
streaming table -> materialized view <- streaming table

**CDC**

Change data capture (CDC) is supported natively in Databricks Delta Lake. There are

TYPE 1: just overwrite old data with new data (no history)
TYPE 2: keep history by adding new records with timerange when they are valid

```SQL
CREATE OR REFRESH STREAMING TABLE customers;

CREATE FLOW scd_type_1_flow AS
AUTO CDC INTO customers
    FROM STREAM updates
    KEYS (CustomerID)
    APPLY AS DELETE WHEN operation = "delete"
    SEQUENCE BY ProcessDate
    COLUMNS * EXCEPT (operation)
    STORED AS SCD TYPE 1;
```

There is also Change Data Feed (CDF) for tracking changes over time (notifiy).

### Assets bundle

Databricks Assets Bundles (DABs):

- code in yaml
- exact definition of Databricks resources 

## 4. Sources

### Jobs

Jobs are workflows, there are divided into **tasks**. Tasks are independent units of work that can be executed in parallel. Order is defined by Directed Acyclic Graph (DAG).

Jobs and tasks has input parameters, output parameters, and dependencies.

Jobs can be triggered manually or automatically based on schedules, or even triggered by events. For example triggered by update some tables. There are even condition triggers.

## 4. Questions


https://www.testpreplab.com/Databricks-Certified-Data-Engineer-Associate-free-practice-test
https://www.examcatalog.com/exam/databricks/certified-data-engineer-associate/

## 5. Sources

TODO:
  liquid clustering

https://airflow.apache.org/
https://www.dask.org/
