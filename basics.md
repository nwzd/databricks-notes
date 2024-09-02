### create delta lake table
```
CREATE TABLE product_info (
  product_id INT,
  product_name STRING,
  category STRING,
  price DOUBLE,
  quantity INT
)
USING DELTA;
```

Explicitly specifying ‘USING DELTA’ identifies Delta Lake as the storage layer for the table, but this clause is optional.

The followinq query returns table details such as number of files, size in bytes, location ..
```
DESCRIBE DETAIL product_info
```
To see files use `fs ls ‘dbfs:/user/hive/warehouse/product_info`

### update delta lake table
```
UPDATE product_info
SET price = price + 10
WHERE product_id = 3
```
The result of fs command will return more files than the table uses. Use describe detail instead of fs if you want to know the true size and num files

### table history
```
DESCRIBE HISTORY product_info
```
each json files under _delta_log is a distinct version of the table
to see details in log file use
```
%fs head 'dbfs:/user/hive/warehouse/product_info/_delta_log/00000000000000000003.json'
```

* Querying by timestamp `SELECT * FROM <table_name> TIMESTAMP AS OF <timestamp>`
* Querying by version number `SELECT * FROM <table_name> VERSION AS OF <version>` or alternatively `SELECT * FROM <table_name>@v<version>`

#### Rollbacking Back to Previous Versions
```
RESTORE TABLE <table_name> TO TIMESTAMP AS OF <timestamp>
RESTORE TABLE <table_name> TO VERSION AS OF <version>
```

### Optimizing Delta Lake Tables
Delta Lake provides an advanced feature for optimizing table performance through compacting small data files into larger ones. This optimization is particularly significant as it enhances the speed of read queries from a Delta Lake table. 
```
OPTIMIZE <table_name>
```
A notable extension of the OPTIMIZE command is the ability to leverage Z-Order indexing. Z-Order indexing involves the reorganization and co-location of column information within the same set of files. To perform Z-Order indexing, you simply add the ZORDER BY keyword to the OPTIMIZE command.
```
OPTIMIZE <table_name>
ZORDER BY <column_names>
```

use VACUUM where certain files become obsolete, either due to uncommitted changes or because they are no longer part of the latest state of the table
```
VACUUM <table_name> [RETAIN num HOURS]
```
The process involves specifying a retention period threshold for the files, so the command will automatically remove all files older than this threshold. The default retention period is set to 7 days, meaning that vacuum operation will prevent you from deleting files less than 7 days old. 
For example, remove all unused, obsolete files
```
SET spark.databricks.delta.retentionDurationCheck.enabled = false
VACUUM product_info RETAIN 0 HOURS
```

While the cleanup operation enhances storage efficiency, it comes at the cost of losing access to older data versions for time travel queries.

## Tables in Databricks
You can create databases outside of the default Hive directory by specifying a custom location using the LOCATION keyword in the CREATE SCHEMA syntax. 
In this case, the database definition still resides in the Hive metastore, but the database folder is located in the specified custom path. 
Tables created within these custom databases will have their data stored in the respective database folder within the custom location

| Managed table | External Table |
| -------- | ------- |
| Created within its own database directory | Created outside the database directory (in a path specified by the LOCATION keyword) |
| `CREATE TABLE table_name` | `CREATE TABLE table_name LOCATION <path>` |
| Dropping the table deletes both the metadata and the underlying data files of the table | Dropping the table only removes the metadata of the table. It does not delete its underlying data files |
We can create an external table in any database, moreover we can create schemas with location:
```
CREATE SCHEMA custom
LOCATION 'dbfs:/Shared/schemas/custom.db';
DESCRIBE DATABASE EXTENDED custom
```
To see if table is managed or external use 
```
DESCRIBE EXTENDED <table_name>
```
To remove underlying files when an external table is deleted: 
```
%python
dbutils.fs.rm('dbfs:/mnt/demo/external_default', True)
```

### CTAS statements
```
CREATE TABLE table_2
AS SELECT * FROM table_1
```
with more options:
```
CREATE TABLE new_users
 COMMENT "Contains PII"
 PARTITIONED BY (city, birth_date)
 LOCATION '/some/path'
 AS SELECT id, name, email, birth_date, city FROM users
```
Partitioning can significantly enhance the performance of large Delta tables by facilitating efficient data retrieval. 
However, it’s important to note that for small to medium-sized tables, the benefits of partition may be negligible or outweighed by drawbacks. 
CTAS statements automate schema declaration by inferring schema information directly from the results of the query.

### Table Constraints
Databricks currently supports two types of table constraints:
NOT NULL constraints, and CHECK constraints.
```
ALTER TABLE table_name ADD CONSTRAINT <constraint_name>  <constraint_detail>
```

When applying constraints to a Delta table, it’s crucial to ensure that existing data in the table adheres to these constraints before defining them. 
Once a constraint is enforced, any new data that violates the constraint will result in a write failure.
CHECK define conditions that incoming data must satisfy in order to be accepted into the table. 
For instance, anyy attempt to insert or update data with dates outside this range will be rejected:
```
ALTER TABLE my_table ADD CONSTRAINT valid_date CHECK (date >= '2024-01-01' AND date <= '2024-12-31');
```

### Cloning Delta Lake Tables
You have two efficient options: deep clone and shallow clone.
Deep clone involves copying both data and metadata from a source table to a target. 
```
CREATE OR REPLACE TABLE table_clone
DEEP CLONE source_table
```

The shallow clone provides a quicker way to create a copy of a table. It only copies the Delta transaction logs, meaning no data movement takes place during shallow cloning.
Shallow cloning is an ideal option for scenarios where, for example, you need to test applying changes on a table without altering the current table’s data. 
This makes it particularly useful in development environments where rapid iteration and experimentation are common.
```
CREATE TABLE table_clone
SHALLOW CLONE source_table
```

### Views
There are three types of views available in Databricks: Stored View, Temporary views, and Global Temporary views.

#### Stored Views
Not a temporary object
```
CREATE VIEW view_tesla_cars
AS SELECT *
   FROM cars
   WHERE brand = 'Tesla';
```
#### Temporary Views
Temporary views are bound to the Spark session and are automatically dropped when the session ends. Since it’s a temporary object, it is not persisted to any database
```
CREATE TEMP VIEW view_name
AS <query>
```

### Global Temporary Views
Global temporary views behave similarly to other temporary views but are tied to the cluster instead of a specific session. Global temporary views are stored in a cluster’s temporary database, named ‘global_temp’.
```
CREATE GLOBAL TEMP VIEW <view_name>
AS <query>;

SELECT * FROM global_temp.<view_name>;
```
#### Comparison of View Types
| Stored Views | Temporary Views | Global Temporary Views |
| -------- | -------- | -------- |
| CREATE VIEW	| CREATE TEMP VIEW | CREATE GLOBAL TEMP VIEW |
| Accessed across sessions/clusters	| Session-scoped | Cluster-scoped |
| Dropped only by DROP VIEW statement | Dropped when session ends	| Dropped when cluster restarted or terminated|
