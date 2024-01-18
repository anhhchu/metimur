# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC A quick start solution to use Databricks Serverless SQL
# MAGIC * Customize data generation
# MAGIC * Benchmark performance using tpch, tpcds, or bring your own data
# MAGIC * If choose Bring your Own Data (BYOD), specify your own catalog and database location
# MAGIC * If using Unity Catalog, specify catalog name and use cluster with UC enabled
# MAGIC
# MAGIC ## Benchmarking options
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
# MAGIC |byod (bring your own data) |customized catalog and database ||

# COMMAND ----------

# MAGIC %md
# MAGIC # Set up

# COMMAND ----------

# MAGIC %pip install -r requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Import utils functions
from utils import *
hostname = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# Specify the benchmarking options
dbutils.widgets.dropdown(name="benchmark", defaultValue="tpch", choices=["tpch", "tpch_sf1", "tpch_sf10", "tpch_sf100", "tpch_sf1000","tpcds_sf10", "tpcds_sf100",  "byod"])

#Specify your query file location
dbutils.widgets.text(name="queryPath", defaultValue="./queries/tpch.sql", label="queryPath")

benchmark = dbutils.widgets.get("benchmark")
warehouseName = dbutils.widgets.get("warehouseName")
queryPath = dbutils.widgets.get("queryPath")

# COMMAND ----------

# Specify name for your warehouse
dbutils.widgets.text(name="warehouseName", defaultValue="🧪 Beaker Benchmark Testing Warehouse", label="warehouseName")
dbutils.widgets.dropdown(name="warehouseType", defaultValue="serverless", choices=["serverless", "pro", "classic"], label="warehouseType")
dbutils.widgets.text(name="concurrency", defaultValue="5", label="concurrency")
dbutils.widgets.dropdown(name="warehouseSize", defaultValue="Small", choices=["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"], label="warehouseSize")
# convert concurrency to integer
concurrency = int(dbutils.widgets.get("concurrency"))
warehouseSize = dbutils.widgets.get("warehouseSize")
warehouseType = dbutils.widgets.get("warehouseType")

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

## TODO: How to grant permission to all users?
# # Grant read permission of public benchmark data (tpch, tpcds) to all users in the workspace
# if benchmark != "byod" and catalog != "samples":
#   print("Grant READ permission")
#   spark.sql(f"GRANT SELECT ON SCHEMA {database} to `account users`")

# COMMAND ----------

# MAGIC %md
# MAGIC # Benchmark

# COMMAND ----------

# Use this cell to detect the set the queryPath to query_file or query_file_dir
if os.path.isdir(queryPath):
    query_file_dir = queryPath
    query_file = None
else:
    query_file_dir = None
    query_file = queryPath

# COMMAND ----------

# Get warehouse id
warehouse_id = get_warehouse(hostname, token, warehouseName)

if warehouse_id:
    # Use your own warehouse
    print(f"Use current warehouse {warehouseName} {warehouse_id}")
    http_path = f"/sql/1.0/warehouses/{warehouse_id}"
    new_warehouse_config = None
else:
    # Specif a new warehouse
    http_path = None
    print(f"Specify new warehouse {warehouseName}")
    new_warehouse_config = {
        "name": warehouseName,
        "type": "warehouse",
        "warehouse": warehouseType,
        "runtime": "latest",
        "size": warehouseSize,
        "min_num_clusters": 1,
        "max_num_clusters": 1,
        "enable_photon": True,
    }


benchmark_configs = {
"name":benchmark,
"db_hostname":hostname,
"token":token,
"query_file":query_file,
"query_file_dir":query_file_dir,
"concurrency":concurrency,
"query_repeat_count":1,
"warehouse_http_path":http_path,
"catalog":catalog,
"schema":database,
"new_warehouse_config":new_warehouse_config,
"results_cache_enabled": True
}

bm = Benchmark(**benchmark_configs)
metrics = bm.execute()

# COMMAND ----------

# MAGIC %md
# MAGIC In Beaker, elapsed_time is calculated by `perf_counter` before and after query execution. It will be slightly different from the query duration from `/api/2.0/sql/history/queries` API

# COMMAND ----------

spark.sql(f"select id, query, float(elapsed_time) as elapsed_time from {benchmark}_vw;").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC In cell below, we obtain the queries history from `/api/2.0/sql/history/queries`  API

# COMMAND ----------

response = get_query_history(hostname=hostname, token=token, warehouse_id=warehouse_id)
get_query_metrics(response, view_name = benchmark)

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
from {benchmark}_hist_view v1
join {benchmark}_vw v2
on v1.query_text = v2.query
"""
).display()

# COMMAND ----------


