# Databricks notebook source
# MAGIC %md
# MAGIC # Set up
# MAGIC * Update parameters in below cell
# MAGIC * Note: 
# MAGIC   * Only work with Unity Catalog and SQL Serverless enabled workspace  
# MAGIC   * If using Unity Catalog, specify catalog name and use cluster with UC enabled

# COMMAND ----------

# Use reload_ext to reload the changes in other files
%reload_ext autoreload
%autoreload 2

# COMMAND ----------

# MAGIC %pip install -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Import utils functions
from utils import *

# COMMAND ----------

hostname = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benchmarking options
# MAGIC
# MAGIC
# MAGIC | Data | Database | Description | Total size |
# MAGIC | --- | --- | --- | --- |
# MAGIC | tpch | samples.tpch | databricks sample data, available in all workspace | ~2GB |
# MAGIC | tpch_sf1 | serverless_benchmark.tpch_sf1 | TPC-H scale factor 1  | ~1GB |
# MAGIC | tpch_sf10 | serverless_benchmark.tpch_sf10 | TPC-H scale factor 10 | ~10GB |
# MAGIC | tpch_sf100 | serverless_benchmark.tpch_sf100 | TPC-H scale factor 100 | ~100GB |
# MAGIC | tpch_sf1000 | serverless_benchmark.tpch_sf1000 | TPC-H scale factor 1000 | ~1TB |
# MAGIC | tpcds_sf10tcl | serverless_benchmark.tpcds_sf10tcl |  | ~10TB |
# MAGIC | tpcds_sf100tcl | serverless_benchmark.tpcds_sf100tcl |  | ~100TB |
# MAGIC |byod (bring your own data) | ||

# COMMAND ----------

# Specify the benchmarking options
dbutils.widgets.dropdown(name="benchmark", defaultValue="tpch", choices=["tpch", "tpch_sf1", "tpch_sf10", "tpch_sf100", "tpch_sf1000","tpcds_sf10", "tpcds_sf100",  "byod"])

# Specify name for your warehouse
dbutils.widgets.text(name="warehouse_name", defaultValue="🧪 Beaker Benchmark Testing Warehouse", label="warehouse_name")

#Specify your schema directory and query file location
dbutils.widgets.text(name="schemaPath", defaultValue="", label="schemaPath")
dbutils.widgets.text(name="queryPath", defaultValue="./queries/tpch.sql", label="queryPath")

benchmark = dbutils.widgets.get("benchmark")
warehouse_name = dbutils.widgets.get("warehouse_name")
schemaPath = dbutils.widgets.get("schemaPath")
queryPath = dbutils.widgets.get("queryPath")

# COMMAND ----------

dbutils.widgets.text(name="concurrency", defaultValue="5", label="concurrency")
dbutils.widgets.dropdown(name="warehouseSize", defaultValue="Small", choices=["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"], label="warehouseSize")
# convert concurrency to integer
concurrency = int(dbutils.widgets.get("concurrency"))
warehouseSize = dbutils.widgets.get("warehouseSize")

# COMMAND ----------

import os
if benchmark == "tpch":
  catalog = "samples"
  database = "tpch"

# BRING YOUR OWN DATA BENCHMARK 
elif benchmark == "byod":
  # Specify your own schema to store your data
  catalog = "serverless_benchmark"
  database = input("Specify the schema to store your own data: ")

# Other benchmarks such as tpc-ds
else:
  catalog = "serverless_benchmark"
  database = benchmark

# COMMAND ----------

# DBTITLE 1,Set up catalog and schema
# USE HIVE METASTORE
if catalog == "hive_metastore":
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{database}")
  spark.sql(f"USE {catalog}.{database}")

# USE UNITY CATALOG
elif catalog != "samples":
  spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
  spark.sql(f"USE catalog {catalog}")
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{database}")
  spark.sql(f"USE {catalog}.{database}")

print(f"Use default {catalog}.{database}")

# COMMAND ----------

# Grant read permission of public benchmark data (tpch, tpcds) to all users in the workspace
if benchmark != "byod" and catalog != "samples":
  print("Grant READ permission")
  spark.sql(f"GRANT SELECT ON SCHEMA {database} to `account users`")

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate data
# MAGIC
# MAGIC Generate data for bring your own data

# COMMAND ----------

# DBTITLE 1,Import schema definition file
table_schemas = import_schema(schemaPath)
tables = list(table_schemas.keys())
tables

# COMMAND ----------

# DBTITLE 1,Set table rows
output_rows = {}
for table in table_schemas.keys():
  rows = input(f"rows for table {table}")
  output_rows[table] = int(rows)

# COMMAND ----------

# DBTITLE 1,Generate Delta Tables
for table in tables:
  sc.setJobDescription(f"Step 1: Generate table {table}") 
  print(f"**Data generation for {table}**")
  delta_path = os.path.join(storage_dir, table)
  generate_delta_table(output_rows[table], table, table_schemas[table], delta_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

for table in tables:
  spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS;")

# COMMAND ----------

# MAGIC %md
# MAGIC # Beaker Benchmark

# COMMAND ----------

if os.path.isdir(queryPath):
    query_file_dir = queryPath
    query_file = None
else:
    query_file_dir = None
    query_file = queryPath

# COMMAND ----------

# Get warehouse id
warehouse_id = get_warehouse(hostname, token, warehouse_name)

if warehouse_id:
    # Use current warehouse
    print(f"Use current warehouse {warehouse_name}")
    http_path = f"/sql/1.0/warehouses/{warehouse_id}"
    new_warehouse_config = None
else:
    # Specify new warehouse
    http_path = None
    print(f"Specify new warehouse {warehouse_name}")
    new_warehouse_config = {
        "name": warehouse_name,
        "type": "warehouse",
        "runtime": "latest",
        "size": warehouseSize,
        "min_num_clusters": 1,
        "max_num_clusters": 1,
        "enable_photon": True,
    }


metrics_view = benchmark

benchmark = Benchmark(
    name=benchmark,
    db_hostname=hostname,
    token=token,
    query_file=query_file,
    query_file_dir=query_file_dir,
    concurrency=concurrency,
    query_repeat_count=1,
    warehouse_http_path=http_path,
    catalog=catalog,
    schema=database,
    new_warehouse_config=new_warehouse_config,
    results_cache_enabled=False
)

metrics = benchmark.execute()

# COMMAND ----------

# MAGIC %md
# MAGIC In Beaker, elapsed_time is calculated by `perf_counter` before and after query execution. It will be slightly different from the query metrics view from `/api/2.0/sql/history/queries` API

# COMMAND ----------

spark.sql(f"select id, query, float(elapsed_time) as elapsed_time from {metrics_view}_vw;").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC In cell below, we obtain the queries history from `/api/2.0/sql/history/queries`  API

# COMMAND ----------

response = get_query_history(hostname=hostname, token=token, warehouse_id=warehouse_id)
get_query_metrics(response, view_name = metrics_view)

# COMMAND ----------

# MAGIC %md
# MAGIC Below graph shows average duration of all queries in the warehouse history (if we run the queries multiple times)

# COMMAND ----------

spark.sql(
    f"""select v2.id, v1.query_text, 
  v1.`duration`/1000 as duration_sec, 
  float(v2. elapsed_time) as elapsed_time,
  v1.`metrics.query_execution_time_ms`/1000 as query_execution_time_sec, 
  v1.`metrics.planning_time_ms`/1000 as planning_time_sec, 
  v1.`metrics.photon_total_time_ms`/1000 as photon_total_time_sec
from {metrics_view}_hist_view v1
join {metrics_view}_vw v2
on v1.query_text = v2.query
"""
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Teardown
# MAGIC Run at the end of the analysis

# COMMAND ----------

# Only remove bring your own data (we keep data of other benchmark to reuse in the workspace)
if benchmark == "byod":
  teardown(database=database)
