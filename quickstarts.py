# Databricks notebook source
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
# MAGIC   * Attach a Databricks personal compute with DBR14.3+ to this notebook
# MAGIC   * Run Each cell under the "Set up" section manually to set up parameters.
# MAGIC 2. Parameters Update: Update the parameters based on your requirements or keep the default values to observe the functionality.
# MAGIC 3. Executing the Notebook: After making the necessary changes, you can click "Run" or "Run All" to execute the entire notebook with the updated parameters.
# MAGIC 4. Warehouses will be stopped right after benchmarking is completed
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC 1. Benchmark Choice:
# MAGIC
# MAGIC * Choose between running the benchmark on a single warehouse ("one-warehouse") or multiple warehouses types("multiple-warehouses") or multiple warehouse sizes ("multiple-warehouses-size").
# MAGIC   * One Warehouse Specification: For the "one-warehouse" option, select a default warehouse specification: 
# MAGIC     * warehouse prefix: This parameter specifies the name prefix of the warehouse. When running the benchmark, the warehouse size and type will be attached to the warehouse prefix before spinning up warehouse
# MAGIC
# MAGIC     * warehouse type: This parameter allows you to select the type of warehouse for the benchmark. The available options are "serverless", "pro", and "classic".
# MAGIC
# MAGIC     * Warehouse Size: This parameter determines the size of the warehouse. You can choose from different predefined sizes such as "2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", and "4X-Large".
# MAGIC
# MAGIC   * Multiple Warehouse Types ("multiple-warehouses"): Running the benchmark on serverless, classic, and pro warehouses with the same size.
# MAGIC   * Multiple Warehouses and Sizes ("multiple-warehouses-size"): Running the benchmark on multiple warehouses of the same type with different sizes. You can choose multiple warehouse sizes from the dropdown **Warehouse Size** widget
# MAGIC
# MAGIC 4. Query Path:
# MAGIC
# MAGIC * Specify the path to the query file or directory containing the benchmark queries.
# MAGIC
# MAGIC * Upload the queries to a separate folder under queries directory, and provide the path in Query Path widget
# MAGIC * IMPORTANT! Ensure your queries follow the specified pattern (put query number between -- and end each query with ;). You can put multiple queries in one file or each query in a separate file.
# MAGIC
# MAGIC ```sql
# MAGIC --q1--
# MAGIC select * from table1;
# MAGIC
# MAGIC --q2--
# MAGIC select * from table2;
# MAGIC ```
# MAGIC
# MAGIC * For queries without params, follow queries/tpch or queries/tpcds folders for example:
# MAGIC   * For **TPCH** benchmark, default Query Path is `queries/tpch`. 
# MAGIC   * For **TPCDS** benchmark, default Query Path is `queries/tpcds`
# MAGIC
# MAGIC * For queries with params, provide params in the queries follow by colon `:param`, then specify the list of params for each query in `params.json` with format {"query_id2": [{param1_name: value11, param2_name: value21}, {param1_name: value12, param2_name: value22}], query_id2: [{param1_name: value11, param2_name: value21}, {param1_name: value12, param2_name: value22}]} on the same folder. Follow examples in `queries/tpch_w_params` folder
# MAGIC
# MAGIC 5. Concurrency Level, Cluster Size, and Result Cache:
# MAGIC
# MAGIC * Query Repetition Count: Determines the number of times each query in the benchmark will be executed.
# MAGIC * Concurrency: Sets the level of concurrency, indicating how many queries can be executed simultaneously.
# MAGIC * Maximum Clusters: Specifies the maximum number of clusters that the warehouse can be scaled up to. It is recommended to use 1 cluster for every 10 concurrent queries.
# MAGIC * Result Cache Enabled (default: False): Determines whether the query will be served from the result cache.

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

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

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
dbutils.widgets.dropdown(name="disk_cache_enabled", defaultValue="True", choices = ["True", "False"], label="14. disk_cache_enabled")

# COMMAND ----------

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

# MAGIC %md
# MAGIC # Benchmark

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

# tables = spark.sql(f"show tables in {catalog_name}.{schema_name}").select("tableName").collect()
# tables = [row["tableName"] for row in tables]
# tables

### List all tables under catalog_name.schema_name using spark.catalog.X api in DBR 14.2 or later
tables_list = spark.catalog.listTables(f"{catalog_name}.{schema_name}")
tables = [table.name for table in tables_list]
tables

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## Benchmark Result

# COMMAND ----------

# MAGIC %md
# MAGIC Below graph shows average duration of all queries in the warehouse history from start to end of benchmark, broken down by warehouses

# COMMAND ----------

display(metrics_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC # View Benchmark Metrics in Lakeview Dashboard
# MAGIC Review the metrics for all your benchmark runs in below dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Modules

# COMMAND ----------

# MAGIC %run ./lakeview_dashboard_gen/benchmark_dash_utils

# COMMAND ----------

# MAGIC %run ./lakeview_dashboard_gen/lakeview_dash_manager

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up Lakeview Parameters
# MAGIC * You will need permission to CREATE CATALOG and CREATE SCHEMA in Unity Catalog to proceed
# MAGIC * Each user will have all benchmark runs saved in Delta table at `serverless_benchmark.default._metimur_metrics_{user_name}`
# MAGIC * All users in the workspace can create table in `serverless_benchmark.default`
# MAGIC * Each user will have their dashboard assets saved in their user workspace location

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
lv_workspace_path = f"/Users/{user_name}"
lv_dashboard_name = "metimur_benchmark_metrics_dashboard"
print(f"Lakeview dashboard assets will be saved at: {lv_workspace_path}")

user_name_clean = user_name.replace(".", "_").replace("@", "_")

lv_metrics_table_name = f"_metimur_metrics_{user_name_clean}"
lv_catalog_name = "serverless_benchmark"
lv_schema_name = "default"

# COMMAND ----------

def set_up_lakeview_catalog(catalog:str, schema:str, table:str):
  spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
  spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `account users`")
  spark.sql(f"GRANT CREATE SCHEMA ON CATALOG {catalog} TO `account users`")
  spark.sql(f"USE catalog {catalog}")
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
  spark.sql(f"GRANT USE SCHEMA ON SCHEMA {schema} TO `account users`")
  spark.sql(f"USE {catalog}.{schema}")
  spark.sql(f"GRANT CREATE TABLE ON SCHEMA {catalog}.{schema} TO `account users`")

  print(f"Your Metrics Data will be saved in {catalog}.{schema}.{table}")

# COMMAND ----------

set_up_lakeview_catalog(lv_catalog_name, lv_schema_name, lv_metrics_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a delta table for Lakeview Dashboard
# MAGIC
# MAGIC Convert pandas dataframe to spark dataframe and save to lakeview table

# COMMAND ----------

metrics_sdf = (
  spark.createDataFrame(metrics_pdf)
                  .withColumn("concurrency", lit(concurrency))
                  .withColumn("benchmark_catalog", lit(catalog_name))
                  .withColumn("benchmark_schema", lit(schema_name))
                  .selectExpr("CAST(current_timestamp() AS STRING) as run_timestamp", "concurrency", 
                              "id", "warehouse_name", "benchmark_catalog", "benchmark_schema",
                              "* except(id, warehouse_name, concurrency, benchmark_catalog, benchmark_schema)")
)
display(metrics_sdf)

create_table_from_df(metrics_sdf, spark, catalog_name=lv_catalog_name, schema_name=lv_schema_name, table_name=lv_metrics_table_name, overwrite=False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create a Lakeview dashboard from the template 
# MAGIC * `./lakeview_dashboard_gen/metimur_benchmark_metrics_dashboard.lvdash.json`

# COMMAND ----------

lv_api = lakeview_dash_manager(host=HOSTNAME, token=TOKEN)
lv_api.load_dash_local("./lakeview_dashboard_gen/metimur_benchmark_metrics_dashboard.lvdash.json")
lv_api.set_query_uc(catalog_name=lv_catalog_name, schema_name=lv_schema_name, table_name=lv_metrics_table_name)
dashboard_link = lv_api.import_dash(path=lv_workspace_path, dashboard_name=lv_dashboard_name)
print(f"Dashboard is ready at: {dashboard_link}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maintain Metrics table

# COMMAND ----------

# Optimize metrics table
spark.sql(f"optimize {lv_catalog_name}.{lv_schema_name}.{lv_metrics_table_name} zorder by (run_timestamp)").display()

# COMMAND ----------

# Desc detail of metrics delta table
spark.sql(f"desc detail {lv_catalog_name}.{lv_schema_name}.{lv_metrics_table_name}").display()

# COMMAND ----------


