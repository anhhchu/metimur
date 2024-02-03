# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC A quick start solution to use Databricks Serverless SQL
# MAGIC * Customize data generation
# MAGIC * Benchmark performance using tpch, tpcds, or bring your own data
# MAGIC * If choose Bring your Own Data (BYOD), specify your own catalog and schema location
# MAGIC * If using Unity Catalog, specify catalog name and use cluster with UC enabled
# MAGIC
# MAGIC ## Benchmarking options
# MAGIC
# MAGIC | Data | schema | Description | Total size |
# MAGIC | --- | --- | --- | --- |
# MAGIC | tpch | samples.tpch | databricks sample data, available in all workspace | ~2GB |
# MAGIC | tpch_sf1 | serverless_benchmark.tpch_sf1 | TPC-H scale factor 1  | ~1GB |
# MAGIC | tpch_sf10 | serverless_benchmark.tpch_sf10 | TPC-H scale factor 10 | ~10GB |
# MAGIC | tpch_sf100 | serverless_benchmark.tpch_sf100 | TPC-H scale factor 100 | ~100GB |
# MAGIC | tpch_sf1000 | serverless_benchmark.tpch_sf1000 | TPC-H scale factor 1000 | ~1TB |
# MAGIC | tpcds_10_gb | hive_metastore.tpcds_1_gb |  | ~1GB |
# MAGIC | tpcds_10_gb | hive_metastore.tpcds_10_gb |  | ~10GB |
# MAGIC |byod (bring your own data) |customized catalog and schema ||

# COMMAND ----------

# MAGIC %md
# MAGIC # Set up

# COMMAND ----------

# MAGIC %pip install -r requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Import utils functions
from utils import *
hostname = spark.conf.get('spark.databricks.workspaceUrl')
token = WorkspaceClient().tokens.create(comment='temp use', lifetime_seconds=60*60*12).token_value
# Specify the benchmarking options
dbutils.widgets.dropdown(name="benchmarks", defaultValue="tpch_samples", choices=["tpch_samples", "tpch_sf1", "tpch_sf10", "tpch_sf100", "tpch_sf1000","tpcds_sf10", "tpcds_sf100",  "byod"])
#Specify your query file location
dbutils.widgets.text(name="queryPath", defaultValue="./queries/tpch.sql", label="queryPath")

benchmarks = dbutils.widgets.get("benchmarks")
warehouseName = dbutils.widgets.get("warehouseName")
queryPath = dbutils.widgets.get("queryPath")

# COMMAND ----------

# Specify name for your warehouse
dbutils.widgets.text(name="warehouseName", defaultValue="🧪 Beaker Benchmark Testing Warehouse", label="warehouseName")
dbutils.widgets.dropdown(name="warehouseType", defaultValue="serverless", choices=["serverless", "pro", "classic"], label="warehouseType")
dbutils.widgets.text(name="concurrency", defaultValue="5", label="concurrency")
dbutils.widgets.dropdown(name="warehouseSize", defaultValue="Small", choices=["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"], label="warehouseSize")
dbutils.widgets.dropdown(name="queryRepetition", defaultValue="10", choices=[str(x) for x in range(1,101)], label="queryRepetition")
# convert concurrency to integer
concurrency = int(dbutils.widgets.get("concurrency"))
queryRepetition = int(dbutils.widgets.get("queryRepetition"))
warehouseSize = dbutils.widgets.get("warehouseSize")
warehouseType = dbutils.widgets.get("warehouseType")
maxCluster = math.ceil(concurrency/10)

# COMMAND ----------

import os
if benchmarks == "tpch_samples":
  catalog = "samples"
  schema = "tpch"

if benchmarks.startswith("tpch"):
  catalog = "tpch"
  schema = benchmarks + "_delta"


elif benchmarks.startswith("tpcds"):
  # Specify your own schema to store your data
  catalog = "tpc-ds"
  schema = benchmarks + "_delta"

# BRING YOUR OWN DATA BENCHMARK 
# Other benchmarks such as tpc-ds
else:
  catalog = "serverless_benchmark"
  schema = benchmarks

# COMMAND ----------

# DBTITLE 1,Set up catalog and schema
# USE HIVE METASTORE
if catalog == "hive_metastore":
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
  spark.sql(f"USE {catalog}.{schema}")

# USE UNITY CATALOG
elif catalog != "samples":
  spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
  spark.sql(f"USE catalog {catalog}")
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
  spark.sql(f"USE {catalog}.{schema}")

print(f"Use default {catalog}.{schema}")

# COMMAND ----------

tables = set(
  spark.sql(f"show tables in {catalog}.{schema}")
  .where("tableName not ILIKE 'benchmark%'")
  .select("tableName")
  .toPandas()["tableName"]
)
tables

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

def run_benchmark(warehouseName, warehouseType):
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
            "max_num_clusters": maxCluster,
            "enable_photon": True,
        }


    benchmark_configs = {
    "name":f"{benchmarks}_{warehouseType}",
    "db_hostname":hostname,
    "token":token,
    "query_file":query_file,
    "query_file_dir":query_file_dir,
    "concurrency":concurrency,
    "query_repeat_count":queryRepetition,
    "warehouse_http_path":http_path,
    "catalog":catalog,
    "schema":schema,
    "new_warehouse_config":new_warehouse_config,
    "results_cache_enabled": False
    }

    bm = Benchmark(**benchmark_configs)
    bm.preWarmTables(tables)
    metrics = bm.execute()

# COMMAND ----------

warehouseName = "ahc_serverless"
warehouseType = "serverless"

run_benchmark(warehouseName, warehouseType)

# COMMAND ----------

warehouseName = "ahc_pro"
warehouseType = "pro"

run_benchmark(warehouseName, warehouseType)

# COMMAND ----------

warehouseName = "ahc_classic"
warehouseType = "classic"

run_benchmark(warehouseName, warehouseType)

# COMMAND ----------

# MAGIC %md
# MAGIC In Beaker, elapsed_time is calculated by `perf_counter` before and after query execution. It will be slightly different from the query duration from `/api/2.0/sql/history/queries` API

# COMMAND ----------

spark.sql(f"select id, query, float(elapsed_time) as elapsed_time from {benchmarks}_vw;").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC In cell below, we obtain the queries history from `/api/2.0/sql/history/queries`  API

# COMMAND ----------

response = get_query_history(hostname=hostname, token=token, warehouse_id=warehouse_id)
get_query_metrics(response, view_name = benchmarks)

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
from {benchmarks}_hist_view v1
join {benchmarks}_vw v2
on v1.query_text = v2.query
"""
).display()

# COMMAND ----------


