# Databricks Serverless SQL Accelerator
![brick](./assets/brick.png)
## About the Accelerator

* Automating data generation and query benchmarking
* Removing dependencies on customer data access
* Suggesting optimization techniques
* Benchmark queries performance in Databricks Serverless SQL

This accelerator utilizes DatabricksLabs [dbldatagen](https://github.com/databrickslabs/dbldatagen) package for DataFrame generation, and [Beaker](https://github.com/goodwillpunning/beaker) package for query benchmarking 

## Overview

The repo structure:
* setup.py: 
   * package installation
   * functions for data generation
   * functions for query performance benchmark
* main.py: generate Delta Tables, analyze DataFrame performance, and execute query benchmarking
* schemas dir: directory for table schemas
* queries dir: directory for benchmark queries 

## Requirements

## Set up

* Step 1. Fork and add your forked repo to a Databricks workspace 
* Step 2: Create a `.env` file from the `.env_template`, provide your DATABRICKS_ACCESS_TOKEN
* Step 3: Clone `schemas/schema_template.csv` to a new directory (refer `schemas/demo` dir for example), and create a new file for each table schema
* Step 4: Create a new file for your queries in `queries` directory following format in `queries/demo_queries.sql`

<details>
<summary>Schema Template Guide</summary>

Refer to `schemas/schema_template.csv` for examples

| field          | format                                                   | description                                                                                                                                                                               | example                                                                                           |
| -------------- | -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| colName        | string                                                   | column name                                                                                                                                                                               | customer_id                                                                                       |
| colType        | string                                                   | data type of a column (string, integer, timestamp, date, float, double, decimal, binary, boolean)                                                                                         | integer                                                                                           |
| minValue       | integer or float                                         | min value for numeric columns                                                                                                                                                             | 1000                                                                                              |
| maxValue       | integer or float                                         | max value for numeric columns                                                                                                                                                             | 1000000                                                                                           |
| step           | integer or float                                         | step size for numeric columns                                                                                                                                                             | 1                                                                                                 |
| uniqueValues   | integer                                                  | number of unique values, used with numeric or string columns                                                                                                                              | 50000                                                                                             |
| distribution   | gamma(shape, size), beta(shape, size), normal(mean, std) | distribution for random value, used with numeric data type                                                                                                                                | gamma(1.0, 2.0), normal(1.0, 2.0), beta(1.0, 2.0)                                                 |
| baseColumn     | comma separated values of strings                        | existing base columns that this column will be dependent on (if specify this value, also specify EITHER \`baseColumnType\` OR \`exp\`)                                                    | event_type, base_minutes                                                                          |
| baseColumnType | string                                                   | specify how this column can be derived from baseColumn, for example, if \`hash\` means that this column is a hash of the base column                                                      | hash                                                                                              |
| exp            | sql expression                                           | sql espression of how this column can be derived from baseColumn                                                                                                                          | case when event_type in ("local call", "ld call", "intl call") then base_minutes else 0 end |
| text           | fakerText                                                | values generated by faker 3rd party library                                                                                                                                               | fakerText("name")                                                                                 |
| format         | string                                                   | format of the column based on the base column injecting with %s. For example, \`%s@example.com\` and baseColumn is customer_name, the customer name will be injected in the string format | %s@example.com                                                                                    |
| values         | comma separated values of strings                        | list of unique values that the column will have, used with string columns, used along with weights                                                                                        | A, B, C, D                                                                                        |
| weights        | comma separated values of integers                       | weights for each string unique values specifying in values. For example, if values are "A, B, C, D" and weights are "30, 20, 10, 10", we have more values of A and B                      | 10, 10, 10, 10                                                                                    |
| template       | string                                                   | template of how the string should be formulated, used with String column                                                                                                                  |
| begin          | timestamp format yyyy-mm-dd hh:mm:ss                     | start time for timestamp or date columns                                                                                                                                                  | 2020-01-01 00:00:00                                                                               |
| end            | timestamp format yyyy-mm-dd hh:mm:ss                     | end time for timestamp or date columns                                                                                                                                                    | 2020-01-31 23:59:59                                                                               |
| interval       | seconds=x, minutes=x, hours=x, days=x, months=x, years=x                   | timestep from start time to end time| seconds=1 |
| random         | FALSE or TRUE                                            | whether the values is random or not, default is FALSE                                                                                                                                     | TRUE                                                                                              |
| omit           | FALSE or TRUE                                            | whether the column will be omitted in the final dataframe, default False; specify True if the column is a baseColumn and shouldn't be visibile in the final dataframe                     | FALSE                                                                                             |
| percentNulls   | float                                                    | percentage of how many null values in the column, default 0.0                                                                                                                             | 0.01                                                                                              |

</details>

## Run the Accelerator
Run the `main.py` notebook in 4 steps:
 * Change parameters in Import Functions cell
 * Generate tables with schemas specified above (Each csv file in the schema dir is used to generate 1 table)
 * Execute manual performance tuning (optional)
 * Run Beaker queries benchmark to test warehouse config and performance




