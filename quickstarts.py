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
# MAGIC 1. Set Up: Run Each cell under the "Set up" section manually to set up parameters.
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
# MAGIC * For **TPCH** benchmark, default Query Path is `queries/tpch`. 
# MAGIC * For **TPCDS** benchmark, default Query Path is `queries/tpcds`
# MAGIC * Query Format: 
# MAGIC   * **IMPORTANT!** Ensure your queries follow the specified pattern (put query number between `--` and end each query with `;`). You can put multiple queries in one file or each query in a separate file. Follow **queries/tpch** or **queries/tpcds** folders for example
# MAGIC
# MAGIC ```sql
# MAGIC --q1--
# MAGIC select * from table1;
# MAGIC
# MAGIC --q2--
# MAGIC select * from table2;
# MAGIC ```
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
from beaker import benchmark
from databricks.sdk import WorkspaceClient
import os
import requests
import re

# COMMAND ----------

logger = logging.getLogger()

HOSTNAME = spark.conf.get('spark.databricks.workspaceUrl')
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
VALID_WAREHOUSES = ["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"]

# COMMAND ----------

# Specify the benchmarking options
dbutils.widgets.dropdown(name="benchmark_choice", defaultValue="one-warehouse", choices=["one-warehouse", "multiple-warehouses", "multiple-warehouses-size"])

dbutils.widgets.text(name="warehouse_prefix", defaultValue="Metimur", label="warehouse_prefix")
dbutils.widgets.dropdown(name="warehouse_type", defaultValue="serverless", choices=["serverless", "pro", "classic"], label="warehouse_type")
dbutils.widgets.multiselect(name="warehouse_size", defaultValue="Small", choices=VALID_WAREHOUSES, label="warehouse_size")

dbutils.widgets.text(name="catalog_name", defaultValue="samples", label="catalog_name")
dbutils.widgets.text(name="schema_name", defaultValue="tpch", label="schema_name")

#Specify your query file location
dbutils.widgets.text(name="query_path", defaultValue="./queries/tpch", label="query_path")
dbutils.widgets.dropdown(name="query_repetition_count", defaultValue="1", choices=[str(x) for x in range(1,101)], label="query_repetition_count")

dbutils.widgets.text(name="concurrency", defaultValue="1", label="concurrency")
dbutils.widgets.text(name="max_clusters", defaultValue="1", label="max_clusters")
dbutils.widgets.dropdown(name="results_cache_enabled", defaultValue="False", choices = ["True", "False"], label="results_cache_enabled")

# COMMAND ----------

benchmark_choice = dbutils.widgets.get("benchmark_choice")

warehouse_prefix = dbutils.widgets.get("warehouse_prefix")
warehouse_size = dbutils.widgets.get("warehouse_size")
warehouse_type = dbutils.widgets.get("warehouse_type")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

query_path = dbutils.widgets.get("query_path")
query_repetition_count = int(dbutils.widgets.get("query_repetition_count"))
concurrency = int(dbutils.widgets.get("concurrency"))
max_clusters = int(dbutils.widgets.get("max_clusters"))
results_cache_enabled = True if dbutils.widgets.get("results_cache_enabled") in ("True", "true") else False


# COMMAND ----------

# MAGIC %md
# MAGIC # Benchmark

# COMMAND ----------

if benchmark_choice == "multiple-warehouses-choice":
  warehouse_sizes = warehouse_size.split(",")
else:
  # Take only the first warehouse option if multiple-warehouses-choice is not selected
  warehouse_sizes = warehouse_size.split(",")
  warehouse_size = warehouse_sizes[0]

print("One warehouse size: ", warehouse_size)
print("Multiple warehouse sizes: ", warehouse_sizes)

# COMMAND ----------

logger.setLevel(logging.WARNING)

tables = spark.catalog.listTables(f"{catalog_name}.{schema_name}")
tables = [table.name for table in tables]
tables

# COMMAND ----------

# reload for changes in benchmark
import importlib
importlib.reload(benchmark)

from beaker import spark_fixture, sqlwarehouseutils
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

logger.setLevel(logging.INFO)

def get_warehouse(hostname, token, warehouse_name):
  sql_warehouse_url = f"https://{hostname}/api/2.0/sql/warehouses"
  response = requests.get(sql_warehouse_url, headers={"Authorization": f"Bearer {token}"})
  
  if response.status_code == 200:
    for warehouse in response.json()['warehouses']:
      if warehouse['name'] == warehouse_name:
        return(warehouse['id'])
  else:
    print(f"Error: {response.json()['error_code']}, {response.json()['message']}")

def run_benchmark(warehouse_type=warehouse_type, warehouse_size=warehouse_size):

    warehouse_name = f"{warehouse_prefix} {warehouse_type} {warehouse_size}"
    # Get warehouse id
    warehouse_id = get_warehouse(HOSTNAME, TOKEN, warehouse_name)

    if warehouse_id:
        # Use your own warehouse
        print(f"--Use current warehouse `{warehouse_name}` {warehouse_id}--")
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"
        new_warehouse_config = None
    else:
        # Specify a new warehouse
        http_path = None
        print(f"--Specify new warehouse `{warehouse_name}`--")
        new_warehouse_config = {
            "name": warehouse_name,
            "type": "warehouse",
            "warehouse": warehouse_type,
            "runtime": "latest",
            "size": warehouse_size,
            "min_num_clusters": 1,
            "max_num_clusters": max_clusters,
            "enable_photon": True,
        }

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

    # bm.preWarmTables(tables)
    
    bm.query_file_format = "semicolon-delimited"
    bm.setConcurrency(concurrency)
    bm.setQueryRepeatCount(query_repetition_count)
    bm.results_cache_enabled = results_cache_enabled

    if os.path.isdir(query_path):
        bm.setQueryFileDir(query_path)
    else:
        bm.setQueryFile(query_path)
    
    metrics_pdf = bm.execute()
    bm.sql_warehouse.close_connection()
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

if benchmark_choice == "one-warehouse":
  metrics_pdf = run_benchmark(warehouse_type)

elif benchmark_choice == "multiple-warehouses":
  metrics_pdf = run_multiple_benchmarks()

elif benchmark_choice == "multiple-warehouses-size":
  metrics_pdf = run_multiple_benchmarks_size(warehouse_sizes)

# COMMAND ----------

# MAGIC %md
# MAGIC Below graph shows average duration of all queries in the warehouse history from start to end of benchmark, broken down by warehouses

# COMMAND ----------

display(metrics_pdf)

# COMMAND ----------

import plotly.graph_objects as go

# Group the metrics by 'id' and 'warehouse_name' and calculate the average duration
grouped_metrics = metrics_pdf.groupby(['id', 'warehouse_name']).mean(numeric_only=True)['duration'].reset_index()

# Create a stacked bar chart using Plotly
fig = go.Figure()

# Iterate over each unique warehouse_name and add a bar for each warehouse
for warehouse_name in grouped_metrics['warehouse_name'].unique():
    warehouse_data = grouped_metrics[grouped_metrics['warehouse_name'] == warehouse_name]
    fig.add_trace(go.Bar(
        x=warehouse_data['id'],
        y=warehouse_data['duration'],
        name=warehouse_name
    ))

# Set the layout of the chart
fig.update_layout(
    xaxis_title='ID',
    yaxis_title='Duration (ms)',
    title='Query Duration by Warehouse'
)

# Display the chart
fig.show()

# COMMAND ----------


