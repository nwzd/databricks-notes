# Transforming Data with Apache Spark

## Querying Data Files
```
select * from file_format.`/path/to/file`;
select * from json.`file_example.json`:
select * from json.`file_*.json`:
select * from json.`file_dir`:
```
file formats: json, parquet, csv, tsv, txt ...
When dealing with multiple files, adding the input_file_name() function becomes useful. This built-in Spark SQL function records the source data file for each record;
```
SELECT *, input_file_name() source_file
 FROM json.`${dataset.school}/students-json`;
```
Instead of reading as json, csv.. you can read directly as text or binary
```
SELECT * FROM text.`path/file.json
SELECT * FROM binaryFile.`path/sample_image.png``
```

Unlike CTAS statements, following approach is particularly useful when dealing with formats that need specific configurations. 
The USING keyword provides increased flexibility by allowing you to specify the type of foreign data source, such as CSV format, as well as any additional files options, such as delimiter and header presence.
```
CREATE TABLE table_name
   (col_name1 col_type1, ...)
USING data_source
OPTIONS (key1 = val1, key2 = val2, ...)
LOCATION path
```
This method creates an external table, serving as a reference to the files without physically moving the data during table creation to the Delta Lake.
Unlike CTAS statements, which automatically infer schema information, creating a table via the USING keyword requires you to provide the schema explicitly.
```
CREATE TABLE csv_external_table
 (col_name1 col_type1, ...)
 USING CSV
 OPTIONS (header = "true",
          delimiter = ";")
 LOCATION = '/path/to/csv/files'
```
Another scenario where the CREATE TABLE statement with the USING keyword proves useful is when creating a table using a JDBC connection:
```
CREATE TABLE jdbc_external_table
USING JDBC
OPTIONS (
 url = 'jdbc:mysql://your_database_server:port',
 dbtable = 'your_database.table_name',
 user = 'your_username',
 password = 'your_password'
);
```

It’s crucial to be aware of the limitations associated with tables having foreign data sources – they are not Delta tables. 
This means that the performance benefits and features offered by Delta Lake, such as time travel and guaranteed access to the most recent version of the data, are not available for these tables.
Unlike Delta Lake tables, which guarantee querying the most recent version of source data, tables registered against other data sources, like CSV, may represent outdated cached data. 
Spark automatically caches the underlying data in local storage for optimal performance in subsequent queries. However, the external CSV file does not natively signal Spark to refresh this cached data. 
Consequently, the new data remains invisible until the cache is manually refreshed using the REFRESH TABLE command.
```
REFRESH TABLE courses_csv
```
However, this action invalidates the table cache, necessitating a rescan of the original data source to reload all data into memory. This process can be particularly time-consuming when dealing with large datasets.
To address this limitation and leverage the advantages of Delta Lake, a workaround involves creating a temporary view that refers to the foreign data source. 
Then, you can execute a CTAS statement on this temporary view to extract the data from the external source and load it into a Delta table. 
```
CREATE TEMP VIEW foreign_source_tmp_vw (col1 col1_type, ...)
 USING data_source
 OPTIONS (key1 = "val1", key2 = "val2", ..., path = "/path/to/files");
 
CREATE TABLE delta_table
AS SELECT * FROM foreign_source_tmp_vw
```

## Writing to Tables
Either by using:CREATE OR REPLACE TABLE statements or INSERT OVERWRITE statements.
```
CREATE OR REPLACE TABLE enrollments AS
SELECT * FROM parquet.`${dataset.school}/enrollments`;

INSERT OVERWRITE enrollments
SELECT * FROM parquet.`${dataset.school}/enrollments`:
```

One significant advantage of using INSERT OVERWRITE is its ability to overwrite only the new records that match the current table schema. This prevents any risk of accidentally modifying the table structure. 
Thus, INSERT OVERWRITE is considered a more secure approach for overwriting existing tables.
When attempting to overwrite data using the INSERT OVERWRITE statement with a schema that differs from the existing table schema, a schema mismatch error will be generated. 
Delta Lake tables are by definition schema-on-write, which means that Delta Lake enforces schema consistency during write operations. 
Any attempt to write data with a schema that differs from the table’s schema will be rejected to maintain data integrity. 
This behavior differs from the first method of CREATE OR REPLACE TABLE statement, which replaces the entire table along with its schema.

### Appending Data
```
INSERT INTO enrollments
SELECT * FROM parquet.`${dataset.school}/enrollments-new`
```
While the INSERT INTO statement provides a convenient means of appending records to tables, it lacks built-in mechanisms to prevent the insertion of duplicate data.

### Merging Data
The MERGE INTO statement enables you to perform upsert operations, meaning we can insert new data, update existing records, and even delete records, all within a single statement. 
```
MERGE INTO students c
USING students_updates u
ON c. student_id = u. student_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
 UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *
```
To delete ```WHEN MATCHED [condition] THEN DELETE```

## Performing Advanced ETL Transformations
Spark SQL facilitates interaction with such JSON data by using a colon syntax (:) to navigate through its nested structures. Example:
```
SELECT student_id, profile:first_name, profile:address:country
FROM students
```
Spark SQL goes further by providing functionality to parse JSON objects into struct types
```
SELECT from_json(profile, <schema>) FROM students;
```
In response to this requirement, we can use the schema_of_json() function, which derives the schema from sample data of the JSON object. Example:
```
SELECT student_id, from_json(profile, schema_of_json('{"first_name":"Sarah", "last_name":"Lundi", "gender":"Female", "address":{"street":"8 Greenbank Road", "city":"Ottawa", "country":"Canada"}}')) AS profile_struct
 FROM students;
```
When working with struct types, a notable aspect is the ability to interact with nested objects using standard period or dot (.) syntax, compared to the colon syntax used for JSON strings.
```
SELECT student_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_students;

SELECT student_id, profile_struct.*
FROM parsed_students;
```
Spark SQL provides dedicated functions for efficiently handling arrays, like the explode() function. This function allows us to transform an array into individual rows, each representing an element from the array.
```
SELECT enroll_id, student_id, explode(courses) AS course
FROM enrollments
```

The collect_set() function is an aggregation function that returns an array of unique values for a given field. It can even deal with fields within arrays.
```
SELECT student_id,
 collect_set(courses.course_id) As before_flatten,
 array_distinct(flatten(collect_set(courses.course_id))) AS after_flatten
FROM enrollments
GROUP BY student_id
```

## Join and Set Operations in Spark SQL
The syntax used for joining data in Spark SQL follows the conventions of standard SQL. We specify the type of join we want (inner, outer, left, right, etc.)
The Union operation in Spark SQL allows us to merge the contents of two datasets, stacking them on top of each other
The Intersect operation, on the other hand, returns the common rows found in both datasets. his operation is particularly useful when identifying overlaps between two datasets. 
```
SELECT * FROM enrollments
INTERSECT
SELECT * FROM enrollments_updates
```
 The minus operation to obtain records exclusive to one dataset.  The minus operation is particularly useful for isolating records of interest
```
SELECT * FROM enrollments
MINUS
SELECT * FROM enrollments_updates
```

## Changing Data Perspectives
Spark SQL supports creating pivot tables for transforming data perspectives using the PIVOT clause. This provides a means to generate aggregated values based on specific column values. 
This transformation results in a pivot table, wherein the aggregated values become multiple columns. 
```
SELECT * FROM (
 SELECT student_id, course.course_id AS course_id, course.subtotal AS subtotal
 FROM enrollments_enriched
)
PIVOT (
 sum(subtotal) FOR course_id IN (
   'C01', 'C02', 'C03', 'C04', 'C05', 'C06',
   'C07', 'C08', 'C09', 'C10', 'C11', 'C12')
```
## Higher Order Functions
The filter function is a fundamental higher order function that enables the extraction of specific elements from an array based on a given lambda function.
```
SELECT
enroll_id,
courses,
FILTER (courses,
       course -> course.discount_percent >= 60) AS highly_discounted_courses
FROM enrollments
```
The transform function is another essential higher order function that facilitates the application of a transformation to each item in an array, extracting the transformed values.
```
SELECT
enroll_id,
courses,
TRANSFORM (
  courses,
  course -> (course.course_id,
             ROUND(course.subtotal * 1.2, 2) AS subtotal_with_tax)
) AS courses_after_tax
FROM enrollments;
```

### Creating UDFs
```
CREATE OR REPLACE FUNCTION gpa_to_percentage(gpa DOUBLE)
RETURNS INT
 
RETURN cast(round(gpa * 25) AS INT)
```

applying
```
SELECT student_id, gpa, gpa_to_percentage(gpa) AS percentage_score
FROM students
```
SQL UDFs are permanent objects stored in the database, allowing them to be used across different Spark sessions and notebooks. The DESCRIBE FUNCTION command provides basic information about the UDF, such as the database, input parameters, and return type.
```
DESCRIBE FUNCTION gpa_to_percentage;
DESCRIBE FUNCTION EXTENDED gpa_to_percentage;
```

more complex udf example:
```
CREATE OR REPLACE FUNCTION get_letter_grade(gpa DOUBLE)
RETURNS STRING
RETURN CASE
        WHEN gpa >= 3.5 THEN "A"
        WHEN gpa >= 2.75 AND gpa < 3.5 THEN "B"
        WHEN gpa >= 2 AND gpa < 2.75 THEN "C"
        ELSE "F"
     END
```

drop udfs:
```
DROP FUNCTION gpa_to_percentage;
DROP FUNCTION get_letter_grade;
```
