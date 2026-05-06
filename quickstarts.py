# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md 
# MAGIC # Instruction
# MAGIC
# MAGIC The notebook provides a convenient way to benchmark and measure query response time across different settings of Databricks SQL Warehouse using [Databricks SQL Connector](https://docs.databricks.com/en/dev-tools/python-sql-connector.html). You can quickly evaluate query performance with varying warehouse sizes or different warehouse types such as Serverless, Pro, or Classic.
# MAGIC
# MAGIC > You should have existing data available in the workspace to proceed. If you don't have available data, the default data used in the notebook is `tpch` data in samples `catalog` along with `tpch` sample queries in `queries` folder of this repo.
# MAGIC
# MAGIC ## Getting Started
# MAGIC
# MAGIC 1. Set Up: 
# MAGIC     * Attach a Databricks personal single-node non-Photon compute with DBR14.3+ to this notebook. 
# MAGIC         * The more concurrency you have the more cores you need in your single node cluster. 
# MAGIC         * Each core can handle 6-8 threads. Adjust this based on the number of warehouses to benchmark against. 
# MAGIC         * For example, if benchmark against 3 warhouses with 20 concurrent queries, you might need at least `3*30/8 ~ 12 cores`
# MAGIC     * Run Each cell under the "Set up" section manually to set up parameters.
# MAGIC 2. Parameters Update: Update the parameters based on your requirements or keep the default values to observe the functionality.
# MAGIC 3. Executing the Notebook: After making the necessary changes, you can click "Run" or "Run All" to execute the entire notebook with the updated parameters.
# MAGIC 4. Warehouses will be stopped right after benchmarking is completed
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC 1. Benchmark Choice:
# MAGIC
# MAGIC     * Choose between running the benchmark on a single warehouse ("one-warehouse") or multiple warehouses types("multiple-warehouses") or multiple warehouse sizes ("multiple-warehouses-size").
# MAGIC     * One Warehouse Specification: For the "one-warehouse" option, select a default warehouse specification: 
# MAGIC       * warehouse prefix: This parameter specifies the name prefix of the warehouse. When running the benchmark, the warehouse size and type will be attached to the warehouse prefix before spinning up warehouse
# MAGIC
# MAGIC       * warehouse type: This parameter allows you to select the type of warehouse for the benchmark. The available options are "serverless", "pro", and "classic".
# MAGIC
# MAGIC       * Warehouse Size: This parameter determines the size of the warehouse. You can choose from different predefined sizes such as "2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", and "4X-Large".
# MAGIC
# MAGIC     * Multiple Warehouse Types ("multiple-warehouses"): Running the benchmark on serverless, classic, and pro warehouses with **the same size**.
# MAGIC     * Multiple Warehouses and Sizes ("multiple-warehouses-size"): Running the benchmark on multiple warehouses of the **same type with different sizes**. You can choose multiple warehouse sizes from the dropdown Warehouse Size widget
# MAGIC
# MAGIC 2. Catalog name, Schema name: 
# MAGIC     * Specify the catalog_name and schema_name location of your existing Delta tables participating in the benchmark
# MAGIC
# MAGIC 3. Query Path & Params Path:
# MAGIC
# MAGIC     * Specify the path to the query file or directory containing the benchmark queries.
# MAGIC
# MAGIC     * Upload the queries to a separate folder under queries directory, and provide the path in Query Path widget
# MAGIC     * IMPORTANT! Ensure your queries follow the specified pattern (put query number between -- and end each query with ;). You can put multiple queries in one file or each query in a separate file.
# MAGIC
# MAGIC         ```sql
# MAGIC         --q1--
# MAGIC         select * from table1;
# MAGIC
# MAGIC         --q2--
# MAGIC         select * from table2;
# MAGIC         ```
# MAGIC
# MAGIC     * For queries without params, follow queries/tpch or queries/tpcds folders for example:
# MAGIC       * For **TPCH** benchmark, default Query Path is `queries/tpch`. 
# MAGIC       * For **TPCDS** benchmark, default Query Path is `queries/tpcds`
# MAGIC       * Set the params_path to an empty value
# MAGIC
# MAGIC     * For queries with params, 
# MAGIC       * Provide params in the queries follow by colon `:param_name`, then specify the list of params for each query in `params.json` with below format in the same folder. Follow examples in `queries/tpch_w_params` folder
# MAGIC         ```json
# MAGIC         {
# MAGIC         "Q01": [ { "l_shipdate": "1998-12-01" }, { "l_shipdate": "1998-11-01" } ],
# MAGIC         "Q02": [ { "p_size": 15, "p_type": "%BRASS", "r_name": "EUROPE" } , 
# MAGIC                 { "p_size": 37, "p_type": "%BRASS", "r_name": "AMERICA" } ],
# MAGIC         }
# MAGIC         ```
# MAGIC       * Specify the path to the params.json in params_path
# MAGIC
# MAGIC 4. Concurrency Level, Cluster Size, and Result Cache:
# MAGIC
# MAGIC     * Query Repetition Count: Determines the number of times each query in the benchmark will be executed.
# MAGIC     * Concurrency: Sets the level of concurrency, indicating how many queries can be executed simultaneously.
# MAGIC     * Min Clusters: Specifies the min number of clusters when starting the warehouse. It is recommended to use 1 cluster for every 10 concurrent queries.
# MAGIC       * For queries that execute quickly, the warehouse may not scale up fast enough. It is advisable to increase the minimum number of clusters.
# MAGIC     * Max Clusters: Specifies the maximum number of clusters that the warehouse can be scaled up to. It is recommended to use 1 cluster for every 10 concurrent queries.
# MAGIC     * Result Cache Enabled (default: False): Determines whether the query will be served from the result cache. For benchmarking purpose, it's recommended to keep this as False

# COMMAND ----------

# MAGIC %md
# MAGIC # Set up

# COMMAND ----------

# MAGIC %pip install -r requirements.txt -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import logging
from beaker import benchmark, spark_fixture, sqlwarehouseutils
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from databricks.sdk import WorkspaceClient
import os
import requests
import re
from pyspark.sql.functions import lit

# COMMAND ----------

logger = logging.getLogger()

# from dbruntime.databricks_repl_context import get_context
# HOSTNAME = get_context().browserHostName
# TOKEN = get_context().apiToken

HOSTNAME = spark.conf.get('spark.databricks.workspaceUrl')
TOKEN = WorkspaceClient().tokens.create(comment='temp use', lifetime_seconds=60*60*12).token_value

VALID_WAREHOUSES = ["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"]

# COMMAND ----------

# Specify the benchmarking options
dbutils.widgets.dropdown(name="benchmark_choice", label="01. benchmark_choice", defaultValue="one-warehouse", choices=["one-warehouse", "multiple-warehouses", "multiple-warehouses-size"])

dbutils.widgets.text(name="warehouse_prefix", defaultValue="Metimur", label="02. warehouse_prefix")
dbutils.widgets.dropdown(name="warehouse_type", defaultValue="serverless", choices=["serverless", "pro", "classic"], label="03. warehouse_type")
dbutils.widgets.multiselect(name="warehouse_sizes", defaultValue="Small", choices=VALID_WAREHOUSES, label="04. warehouse_sizes")

dbutils.widgets.text(name="catalog_name", defaultValue="samples", label="05. catalog_name")
dbutils.widgets.text(name="schema_name", defaultValue="tpch", label="06. schema_name")

#Specify your query file location
dbutils.widgets.text(name="query_path", defaultValue="queries/tpch_w_params", label="07. query_path")
dbutils.widgets.text(name="params_path", defaultValue="./queries/tpch_w_params/params.json", label="08. params_path")
dbutils.widgets.dropdown(name="query_repetition_count", defaultValue="1", choices=[str(x) for x in range(1,101)], label="09. query_repetition_count")

dbutils.widgets.text(name="concurrency", defaultValue="1", label="10. concurrency")
dbutils.widgets.text(name="min_clusters", defaultValue="1", label="11. min_clusters")
dbutils.widgets.text(name="max_clusters", defaultValue="1", label="12. max_clusters")
dbutils.widgets.dropdown(name="results_cache_enabled", defaultValue="False", choices = ["True", "False"], label="13. results_cache_enabled")
dbutils.widgets.dropdown(name="disk_cache_enabled", defaultValue="False", choices = ["True", "False"], label="14. disk_cache_enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC # Benchmark

# COMMAND ----------

# DBTITLE 1,Confirm the parameters below
# List all widget names and their values
widgets = dbutils.widgets.getAll()

# Create variables with the same names as the widget names and assign their values
for name, value in widgets.items():
    if name in ["query_repetition_count", "concurrency", "min_clusters", "max_clusters"]:
        exec(f"{name} = int('{value}')")
    elif name in ["results_cache_enabled", "disk_cache_enabled"]:
        exec(f"{name} = True if '{value}' in ('True', 'true') else False")
    else:
        exec(f"{name} = '{value}'")

# Print the variables to verify
for name, value in widgets.items():
    print(f"{name}: {eval(name)}")

# COMMAND ----------

warehouse_sizes = warehouse_sizes.split(",")
warehouse_size = warehouse_sizes[0]
if benchmark_choice == "multiple-warehouses-size":
  print("Benchmark on multiple warehouse sizes:", warehouse_sizes)
elif benchmark_choice == "multiple-warehouses":
  print("Benchmark on multiple warehouse types PRO, CLASSIC, SERVERLESS of size:", warehouse_size)
elif benchmark_choice == "one-warehouse":
  # Take only the first warehouse option if multiple-warehouses-size is not selected
  print("Benchmark on One warehouse of size:", warehouse_size)

# COMMAND ----------

# DBTITLE 1,List tables
# tables = spark.sql(f"show tables in {catalog_name}.{schema_name}").select("tableName").collect()
# tables = [row["tableName"] for row in tables]
# tables

### List all tables under catalog_name.schema_name using spark.catalog.X api in DBR 14.2 or later
tables_list = spark.catalog.listTables(f"{catalog_name}.{schema_name}")
tables = [table.name for table in tables_list]
tables

# COMMAND ----------

# DBTITLE 1,Get warehouse
def get_warehouse(hostname, token, warehouse_name):
  sql_warehouse_url = f"https://{hostname}/api/2.0/sql/warehouses"
  response = requests.get(sql_warehouse_url, headers={"Authorization": f"Bearer {token}"})
  
  if response.status_code == 200:
    for warehouse in response.json()['warehouses']:
      if warehouse['name'] == warehouse_name:
        return(warehouse['id'])
  else:
    print(f"Error: {response.json()['error_code']}, {response.json()['message']}")

def update_warehouse(hostname, token, warehouse_id, new_config):
    sql_warehouse_url = f"https://{hostname}/api/2.0/sql/warehouses/{warehouse_id}/edit"
    response = requests.post(sql_warehouse_url, headers={"Authorization": f"Bearer {token}"}, json=new_config)
    
    if response.status_code == 200:
        print(f"Warehouse {warehouse_id} updated successfully.")
    else:
        print(f"Error: {response.json()['error_code']}, {response.json()['message']}")

# COMMAND ----------

# DBTITLE 1,Run benchmark
def run_benchmark(warehouse_type=warehouse_type, warehouse_size=warehouse_size):

    warehouse_name = f"{warehouse_prefix} {warehouse_type} {warehouse_size}"
    # Get warehouse id
    warehouse_id = get_warehouse(HOSTNAME, TOKEN, warehouse_name)

    new_warehouse_config = {
            "name": warehouse_name,
            "type": "warehouse",
            "warehouse": warehouse_type,
            "runtime": "latest",
            "size": warehouse_size,
            "min_num_clusters": min_clusters,
            "max_num_clusters": max_clusters,
            "enable_photon": True,
        }

    if warehouse_id:
        # Update existing warehouse
        print(f"--Updating current warehouse `{warehouse_name}` {warehouse_id}--")
        update_warehouse(HOSTNAME, TOKEN, warehouse_id, new_warehouse_config)
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"
        new_warehouse_config = None
    else:
        # Specify a new warehouse
        http_path = None
        print(f"--Specify new warehouse `{warehouse_name}`--")
        

    bm = benchmark.Benchmark()
    bm.setName(f"Benchmark {warehouse_name}")
    bm.setHostname(HOSTNAME)
    bm.setWarehouseToken(TOKEN)

    if http_path:
        bm.setWarehouse(http_path)
    else:
        bm.setWarehouseConfig(new_warehouse_config)


    bm.setCatalog(catalog_name)
    bm.setSchema(schema_name)

    if disk_cache_enabled:
        bm.preWarmTables(tables)
    
    bm.setConcurrency(concurrency)
    bm.setQueryRepeatCount(query_repetition_count)
    bm.results_cache_enabled = results_cache_enabled

    if os.path.isdir(query_path):
        bm.setQueryFileDir(query_path)
    else:
        bm.setQueryFile(query_path)
    
    if params_path:
        bm.setParamsPath(params_path)

    metrics_pdf = bm.execute()
    # bm.sql_warehouse.close_connection()
    bm.stop_warehouse(bm.warehouse_id)
    return  metrics_pdf


def run_multiple_benchmarks():
    """
    Run multiple benchmarks for different warehouse types.
    
    Returns:
    - combined_metrics_pdf (pandas.DataFrame): A Pandas DataFrame containing the combined metrics results from all the benchmarks.
    """

    with ThreadPoolExecutor(max_workers=3) as executor:
        warehouse_types = ["serverless", "pro", "classic"]
        futures = [executor.submit(run_benchmark, warehouse_type, warehouse_size) for warehouse_type in warehouse_types]
        wait(futures, return_when=ALL_COMPLETED)
    
    combined_metrics_pdf = pd.DataFrame()
    for future in futures:
        if combined_metrics_pdf.empty:
            combined_metrics_pdf = future.result()
        else:
            combined_metrics_pdf = pd.concat([combined_metrics_pdf, future.result()])

    return combined_metrics_pdf

def run_multiple_benchmarks_size(warehouse_sizes):
    """
    Run multiple benchmarks for different warehouse sizes.
    
    Parameters:
    - warehouse_sizes (list): A list of warehouse sizes to be benchmarked.
    
    Returns:
    - combined_metrics_pdf (pandas.DataFrame): A Pandas DataFrame containing the combined metrics results from all the benchmarks.
    """

    with ThreadPoolExecutor(max_workers=len(warehouse_sizes)) as executor:
        futures = [executor.submit(run_benchmark, warehouse_type, warehouse_size) for warehouse_size in warehouse_sizes]
        wait(futures, return_when=ALL_COMPLETED)
    
    combined_metrics_pdf = pd.DataFrame()
    for future in futures:
        if combined_metrics_pdf.empty:
            combined_metrics_pdf = future.result()
        else:
            combined_metrics_pdf = pd.concat([combined_metrics_pdf, future.result()])

    return combined_metrics_pdf

# COMMAND ----------

# reload for changes in benchmark
import importlib
importlib.reload(benchmark)

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

if benchmark_choice == "one-warehouse":
  metrics_pdf = run_benchmark(warehouse_type)

elif benchmark_choice == "multiple-warehouses":
  metrics_pdf = run_multiple_benchmarks()

elif benchmark_choice == "multiple-warehouses-size":
  metrics_pdf = run_multiple_benchmarks_size(warehouse_sizes)

# COMMAND ----------

from datetime import datetime, timezone

start_unix_time_ms = 1778000624018
end_unix_time_ms = 1778001506176
# Convert milliseconds to seconds
query_start_from = datetime.fromtimestamp(start_unix_time_ms / 1000, timezone.utc)
query_start_to = datetime.fromtimestamp(end_unix_time_ms / 1000, timezone.utc)

# print(timestamp.strftime('%Y-%m-%d %H:%M:%S'))
print(query_start_from, query_start_to)

# COMMAND ----------

# MAGIC %md
# MAGIC # Benchmark Result

# COMMAND ----------

# DBTITLE 1,System Table Method
# MAGIC %sql
# MAGIC -- If system.billing.attributed_usage system table is available in the workspace (Workspace is enrolled in Attributed Usage System Table private preview)
# MAGIC -- it might take up to 24-48hr for data availability
# MAGIC
# MAGIC SELECT
# MAGIC   qh.statement_id,
# MAGIC   qh.account_id,
# MAGIC   qh.workspace_id,
# MAGIC   qh.executed_by,
# MAGIC   qh.statement_text,
# MAGIC   qh.compute.warehouse_id AS warehouse_id,
# MAGIC   qh.execution_status,
# MAGIC   COALESCE(qh.client_application, 'Unknown') AS client_application,
# MAGIC   qh.start_time,
# MAGIC   qh.end_time,
# MAGIC   qh.total_duration_ms,
# MAGIC   qh.waiting_for_compute_duration_ms as queue_time,
# MAGIC   qh.total_task_duration_ms,
# MAGIC   qh.compilation_duration_ms,
# MAGIC   qh.result_fetch_duration_ms,
# MAGIC   au.usage_date,
# MAGIC   au.usage_unit,
# MAGIC   p.pricing.effective_list.`default` as list_price,
# MAGIC   active_usage_quantity,
# MAGIC   (CAST(p.pricing.effective_list.`default` AS FLOAT) * au.active_usage_quantity) AS usage_dollars,
# MAGIC   au.sku_name
# MAGIC FROM
# MAGIC   system.query.history qh
# MAGIC     LEFT JOIN system.billing.attributed_usage au
# MAGIC       on au.usage_metadata.dbsql_statement_id = qh.statement_id
# MAGIC     LEFT JOIN system.billing.list_prices p
# MAGIC       ON au.sku_name = p.sku_name
# MAGIC       AND au.start_time BETWEEN
# MAGIC         p.price_start_time
# MAGIC       AND
# MAGIC         coalesce(p.price_end_time, date_add(current_date, 1))
# MAGIC WHERE
# MAGIC   qh.workspace_id = :workspace_id
# MAGIC   and qh.executed_by = :executed_by
# MAGIC   and qh.compute.warehouse_id = :warehouse_id
# MAGIC   and qh.start_time between :query_start_from and :query_start_to;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Per Query (Event Driven) — Method Details
# MAGIC This method calculates the true financial cost of a query using an event-driven, "top-down" approach. It starts with the actual billed DBUs from the Databricks system.billing.usage table and proportionately distributes those costs down to individual queries based on warehouse events (uptime, scaling).
# MAGIC
# MAGIC * **Cost is dependent on concurrent queries** — The same query will be attributed different cost depending on warehouse concurrency at execution time. Low concurrency → higher per-query cost. High concurrency → lower per-query cost. Snowflake's QUERY_ATTRIBUTION_HISTORY exhibits the exact same concurrency dependency. This is not a limitation — it's a fundamental, mathematically unavoidable property of any proportional allocation model on shared compute. 
# MAGIC * **Cost can fluctuate by analysis run time** 
# MAGIC
# MAGIC
# MAGIC **Formula:**
# MAGIC ```
# MAGIC utilization_proportion = utilized_seconds / (utilized_seconds + idle_seconds)
# MAGIC query_attributed_dbus  = utilization_proportion × total_dbus × query_task_time_proportion
# MAGIC ```
# MAGIC
# MAGIC * **Idle detection:** Uses `system.compute.warehouse_events` to build ON/OFF timeline, cross-references with query activity. Classifies each second as UTILIZED / ON\_IDLE / OFF. Denominator is **ON-time only** (OFF seconds excluded).
# MAGIC * **Work metric:** `total_task_duration_ms + result_fetch_duration_ms + compilation_duration_ms`.
# MAGIC * **Query timing:** Adjusted `query_work_start_time` (excludes wait) / `query_work_end_time` (includes fetch).
# MAGIC * **Architecture:** Warehouse-focused — processes all queries in the boundary window → builds full utilization timeline → attributes cost per hour bucket.
# MAGIC
# MAGIC | Pros | Cons |
# MAGIC | --- | --- |
# MAGIC | More precise utilization via warehouse events | More complex, harder to debug |
# MAGIC | Broader work metric captures full query footprint | Higher cost estimates (may over-attribute) |
# MAGIC | Accounts for actual query work windows | Heavier to run (second-level granularity) |
# MAGIC | Designed for batch / MV materialization | Results can shift as new data arrives |
# MAGIC | Rich query source classification | |
# MAGIC
# MAGIC
# MAGIC ### Filter Behavior
# MAGIC
# MAGIC * **`:executed_by`, `:statement_id`, and time range filters** — Applied only to the **final output** in both cells. They do NOT affect the cost calculation. All concurrent queries on the warehouse are always considered when computing proportional cost.
# MAGIC * **`:warehouse_id` filter** — Applied **early** in both cells to reduce scan size. Safe because utilization is computed per-warehouse.
# MAGIC
# MAGIC
# MAGIC ### 2. Treatment of Concurrent Queries
# MAGIC
# MAGIC Because this is an event-driven model based on actual warehouse uptime, it inherently uses a shared cost philosophy.
# MAGIC
# MAGIC * High Concurrency: If a warehouse costs $1.00 per minute to run, and 20 queries are running concurrently for that minute, the $1.00 is split among them based on their relative resource weight. The cost per query drops significantly because the warehouse overhead is shared across many tasks.
# MAGIC
# MAGIC * Low Concurrency / Idle Time: If only a single query is running—or if the warehouse remains warm and active between sporadic queries—the active queries must absorb the "warehouse tax" (the full cost of the warehouse being powered on, even if the hardware is underutilized).
# MAGIC
# MAGIC ### Treatment of Compilation Time & Result Fetching Time
# MAGIC
# MAGIC #### Where These Operations Run
# MAGIC
# MAGIC | Operation | Snowflake | Databricks |
# MAGIC | --- | --- | --- |
# MAGIC | **Compilation / Optimization** | Runs on the **Cloud Services layer** (separate from warehouse) | Runs on the **SQL Warehouse compute** (consumes DBUs) |
# MAGIC | **Result Fetching** | Runs on the **Cloud Services layer** (result cache → client transfer) | Runs on the **SQL Warehouse compute** (consumes DBUs) |
# MAGIC | **Execution** | Runs on the **Virtual Warehouse** (consumes credits) | Runs on the **SQL Warehouse compute** (consumes DBUs) |
# MAGIC
# MAGIC Snowflake offloads compilation and result fetching to its Cloud Services layer, which is billed separately from warehouse compute credits. Databricks runs all three phases on the warehouse itself, meaning all three consume DBUs within the same billing stream.
# MAGIC
# MAGIC
# MAGIC #### Impact on Cross-Platform Cost Comparison
# MAGIC
# MAGIC Cost per Query cell above captures the complete warehouse footprint including compilation and fetch, which are real DBU consumers on Databricks. Not comparable to Snowflake's warehouse-only metric.
# MAGIC
# MAGIC
