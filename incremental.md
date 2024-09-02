# Streaming Data with Apache Spark

## What is a Data Stream ?
A data stream represents a continuous flow of data that evolves or accumulates over time, often originating from various sources such as sensors, log files, or social media platforms. 
As new data is generated, it is appended to the stream, making it a dynamic and constantly changing dataset.

Processing data streams present a unique set of challenges due to their dynamic and ever-growing nature. To handle such continuous flows of information, there are typically two primary approaches:
* Recompute: In this classical approach, each time new data arrives, the entire dataset is reprocessed to incorporate the new information. While this method ensures accuracy, it can be computationally intensive and time-consuming, especially for large datasets.
* Incremental Processing: Alternatively, incremental processing involves developing custom logic to identify and capture only the new data that has been added since the last update. This approach reduces processing overhead by focusing solely on the changes, thereby improving efficiency.

### Spark Structured Streaming
The core concept in Structured Streaming is to treat a live data stream as a table that is being continuously appended.
By using the ‘spark.readStream’ method in Python, you can easily query a Delta Lake table as a streaming source. 
```
streamDF = spark.readStream.table("source_table")
```
Once the necessary transformations have been applied, the results of the streaming DataFrame can be persisted using its ‘writeStream’ method.
```
streamDF.writeStream.table("target_table")
```

For example, we have two Delta Lake tables, ‘Table_1’ and ‘Table_2’. The goal is to continuously stream data from ‘Table_1’ to ‘Table_2', appending new records into ‘Table_2’ every 2 minutes
```
streamDF = spark.readStream
                .table("Table_1")
streamDF.writeStream
   .trigger(processingTime="2 minutes")
   .outputMode("append")
   .option("checkpointLocation", "/path")
   .table("Table_2")
```
| Mode | Usage | Behavior |
| ----- | ----- | ----- |
| Continuous | `.trigger(processingTime= "5 minutes")` | Process data at fixed intervals (e.g., every 5 minutes). Default Interval: 500ms |
| Triggered | `.trigger(once=True)` | Process all available data in a single micro-batch, then stop automatically. It may lead to out-of-memory (OOM) errors with large volumes of data|
| Triggered | `.trigger(availableNow=True)` | Process all available data in multiple micro-batches, then stop automatically.|

Since Databricks Runtime 11.3 LTS, the Trigger.Once setting has been deprecated. Databricks now recommends using Trigger.AvailableNow for all incremental batch processing workloads.

Output modes:

| Mode | Usage | Behavior |
| ----- | ----- | ----- |
| Append (default) | `.outputMode("append")` | Only newly received rows are appended to the target table with each batch |
| Complete | `.outputMode("complete")` | This mode is commonly used for updating summary tables with the latest aggregates. |

Checkpointing ensures that if the streaming job crashes or needs to be restarted, it can resume processing from the last checkpointed state rather than starting from scratch.
Each streaming write operation requires its own separate checkpoint location.

Structured Streaming Guarantees
* Fault Recovery: is achieved through the combination of checkpointing and a mechanism called write-ahead logs which make it possible to recover from failures without any data loss.
* Exactly-once Semantics: guarantees that each record in the stream will be processed exactly once

## Incremental Data Ingestion
Databricks offers two efficient mechanisms for the incremental processing of newly arrived files in a storage location: the Copy Into SQL command and Auto Loader

### COPY INTO Command
This command operates in an idempotent and incremental manner, meaning that each execution will only process new files from the source location, while previously ingested files are ignored.
```
COPY INTO my_table
FROM '/path/to/files’
FILEFORMAT = CSV
FORMAT_OPTIONS ('delimiter' = '|’,
             'header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true’)
```
### Auto Loader
Built upon Spark’s Structured Streaming framework, Auto Loader employs checkpointing to track the ingestion process and store metadata information about the discovered files. 
This ensures that data files are processed exactly once by Auto Loader. Moreover, in the event of a failure, Auto Loader seamlessly resumes processing from the point of interruption.
```
spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", <source_format>)
         .load('/path/to/files’)
     .writeStream
         .option("checkpointLocation", <checkpoint_directory>)
         .table(<table_name>)
```

Auto Loader offers a convenient feature that enables automatic schema detection for loaded data, allowing you to create tables without explicitly defining the data schema. 
Moreover, if new columns are added, the table schema can evolve accordingly. However, to avoid inference costs during each stream startup, the inferred schema can be stored for subsequent use. 
This is achieved by specifying a location where Auto Loader can store the schema using the ‘cloudFiles.schemaLocation’ option.
With typed schemas, such as Parquet, Auto Loader extracts the predefined schemas from the files. On the other hand, for formats that don’t encode data types, like JSON and CSV, Auto Loader infers all columns as strings by default. 
To enable inferring column data types from such sources, you can set the option ‘cloudFiles.inferColumnTypes’ to true.
```
spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", <source_format>)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", <schema_directory>)
        .load('/path/to/files’)
    .writeStream
        .option("checkpointLocation", <checkpoint_directory>)
        .option("mergeSchema", "true")
        .table(<table_name>)
```
It’s worth mentioning that the designated schema location can be identical to the checkpoint location for simplicity and convenience.
### Comparison of Ingestion Mechanisms
| COPY INTO | Autoloader |
| --------- | ---------- |
| Thousands of files	| Millions of files |
| Less efficient at scale	| Efficient at scale |

### Multi-Hop Architecture
The multi-hop architecture, also referred to as the Medallion architecture, is a data design pattern that logically organizes data in a multi-layered approach. 
* Bronze Layer: At this layer, data is ingested from external systems and stored in its rawest form
* Silver Layer: This middle layer focuses on data cleansing, normalization, and validation.
* Gold Layer: This layer is characterized by its role in facilitating high-level business analytics and intelligence. Data at this stage is often aggregated and summarized to support specific business needs

Note: Structured Streaming assumes data is only being appended in the upstream tables. Once a table is updated or overwritten, it becomes invalid for streaming reads. Therefore, reading a stream from such a Gold table is not supported. 
To alter this behavior, options like ‘skipChangeCommits’ can be utilized, although they may come with other limitations that need to be considered. (https://docs.databricks.com/en/structured-streaming/delta-lake.html#ignore-updates-and-deletes)
