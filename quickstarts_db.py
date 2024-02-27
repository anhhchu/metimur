# Databricks notebook source
# MAGIC %md 
# MAGIC # Purpose
# MAGIC
# MAGIC This quickstart notebook demonstrates the usage of the [Databricks SQL Connector](https://docs.databricks.com/en/dev-tools/python-sql-connector.html) to execute queries concurrently.
# MAGIC
# MAGIC The notebook provides a convenient way to benchmark and measure query response time across different configurations of SQL Severless Warehouse. You can evaluate query performance with varying warehouse sizes or different warehouse types such as Serverless, Pro, or Classic.
# MAGIC
# MAGIC > You should have existing data available in the workspace to proceed. If you don't have available data, the default data used in the notebook is tpch data in samples catalog along with tpch sample queries in queries folder of this repo. If you want to use TPCH and TPCDS data with different scale factor, or generate your own data with defined schema, go to Advanced section.
# MAGIC
# MAGIC ## Getting Started
# MAGIC
# MAGIC 1. Run the "Set up" cells below.
# MAGIC 2. Update the parameters based on your purpose or keep the default values to see how it works.
# MAGIC 3. After making the necessary changes, click "Run" or "Run All" to execute the entire notebook with the updated parameters.
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC * benchmarks: (please ignore)
# MAGIC
# MAGIC * benchmark_choice: This parameter allows you to choose between running the benchmark on a single warehouse ("one-warehouse") or multiple warehouses ("multiple-warehouses"). The default warehouse specification (for `one-warehouse` option) can be chosen from below. 
# MAGIC
# MAGIC   * If you choose `multiple-warehouses` option, you will run benchmark on serverless, classic, pro warehouse with the same size
# MAGIC   
# MAGIC   * If you choose `multiple-warehouses-size` option, you will run benchmark on multiple warehouses with different sizes. You will have the option to specify the warehouse sizes in Cell 7 on this notebook
# MAGIC
# MAGIC Specify the warehouse info:
# MAGIC
# MAGIC * warehouse_prefix: This parameter specifies the name prefix of the warehouse. When running the benchmark, the warehouse size and type will be attached to the warehouse_name before spinning up warehouse
# MAGIC
# MAGIC * warehouse_type: This parameter allows you to select the type of warehouse for the benchmark. The available options are "serverless", "pro", and "classic".
# MAGIC
# MAGIC * warehouse_size: This parameter determines the size of the warehouse. You can choose from different predefined sizes such as "2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", and "4X-Large".
# MAGIC
# MAGIC Specify the location of your existing data below:
# MAGIC
# MAGIC * catalog_name: This parameter specifies the name of the catalog where the benchmark schema is located.
# MAGIC
# MAGIC * schema_name: This parameter defines the name of the schema within the catalog where the benchmark tables are stored.
# MAGIC
# MAGIC Upload your `queries` to queries folder, and provide the query path below:
# MAGIC
# MAGIC * query_path: This parameter specifies the path to the query file or directory containing the benchmark queries.
# MAGIC
# MAGIC Specify the concurrency level, cluster size, and whether to enable result cache:
# MAGIC
# MAGIC * query_repetition_count: This parameter determines the number of times each query in the benchmark will be executed.
# MAGIC
# MAGIC * concurrency: This parameter sets the level of concurrency, indicating how many queries can be executed simultaneously.
# MAGIC
# MAGIC * max_clusters: This parameter specifies the maximum number of clusters that the warehouse can be scaled up to. We recommend 1 cluster for 10 concurrent queries (maximum 25 clusters)
# MAGIC
# MAGIC * results_cache_enabled (default False): if False the query won't be served from result cache

# COMMAND ----------

# MAGIC %md
# MAGIC # Set up

# COMMAND ----------

# MAGIC %pip install -r requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Import utils functions
from utils.func import *
import pandas as pd
import logging

logger = logging.getLogger()

HOSTNAME = spark.conf.get('spark.databricks.workspaceUrl')
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
VALID_WAREHOUSES = ["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"]

# COMMAND ----------

# Specify the benchmarking options
dbutils.widgets.dropdown(name="benchmarks", defaultValue="ODBC", choices=["ODBC"])
dbutils.widgets.dropdown(name="benchmark_choice", defaultValue="one-warehouse", choices=["one-warehouse", "multiple-warehouses", "multiple-warehouses-size"])

dbutils.widgets.text(name="warehouse_prefix", defaultValue="Benchmarking", label="warehouse_prefix")
dbutils.widgets.dropdown(name="warehouse_type", defaultValue="serverless", choices=["serverless", "pro", "classic"], label="warehouse_type")
dbutils.widgets.dropdown(name="warehouse_size", defaultValue="Small", choices=VALID_WAREHOUSES, label="warehouse_size")

dbutils.widgets.text(name="catalog_name", defaultValue="samples", label="catalog_name")
dbutils.widgets.text(name="schema_name", defaultValue="tpch", label="schema_name")

#Specify your query file location
dbutils.widgets.text(name="query_path", defaultValue="./queries/tpch", label="query_path")
dbutils.widgets.dropdown(name="query_repetition_count", defaultValue="10", choices=[str(x) for x in range(1,101)], label="query_repetition_count")

dbutils.widgets.text(name="concurrency", defaultValue="5", label="concurrency")
dbutils.widgets.text(name="max_clusters", defaultValue="5", label="max_clusters")
dbutils.widgets.dropdown(name="results_cache_enabled", defaultValue="False", choices = ["True", "False"], label="results_cache_enabled")

# COMMAND ----------

benchmarks = dbutils.widgets.get("benchmarks")
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
results_cache_enabled = False if dbutils.widgets.get("results_cache_enabled") == "False" else True

# COMMAND ----------

if benchmark_choice == "multiple-warehouses-size":
  print("Enter warehouse sizes separates by comma (ie. 2X-Small, X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large)")
  sizes = input("warehouse sizes: ")
  warehouses = set([size.strip() for size in sizes.strip().split(",")])
  invalid_warhouses = warehouses - set(VALID_WAREHOUSES)
  assert len(invalid_warhouses) == 0, "Please rerun this cell & enter only valid warehouses in the list"
  warehouse_sizes = warehouses & set(VALID_WAREHOUSES)
  print(warehouse_sizes)

# COMMAND ----------

# MAGIC %md
# MAGIC # Benchmark

# COMMAND ----------

logger.setLevel(logging.WARNING)

tables = (spark.sql(f"show tables in {catalog_name}.{schema_name}")
                .where("isTemporary = 'false'")
                .select("tableName")
                .toPandas()["tableName"].tolist()
          )
tables

# COMMAND ----------

from beaker import spark_fixture, sqlwarehouseutils
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

logger.setLevel(logging.INFO)

def run_benchmark(warehouse_prefix, warehouse_type=warehouse_type, warehouse_size=warehouse_size):

    warehouse_prefix = f"{warehouse_prefix} {warehouse_type} {warehouse_size}"
    # Get warehouse id
    warehouse_id = get_warehouse(HOSTNAME, TOKEN, warehouse_prefix)

    if warehouse_id:
        # Use your own warehouse
        print(f"--Use current warehouse `{warehouse_prefix}` {warehouse_id}--")
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"
        new_warehouse_config = None
    else:
        # Specify a new warehouse
        http_path = None
        print(f"--Specify new warehouse `{warehouse_prefix}`--")
        new_warehouse_config = {
            "name": warehouse_prefix,
            "type": "warehouse",
            "warehouse": warehouse_type,
            "runtime": "latest",
            "size": warehouse_size,
            "min_num_clusters": 1,
            "max_num_clusters": max_clusters,
            "enable_photon": True,
        }

    bm = Benchmark()
    bm.setName(f"{benchmarks}_{warehouse_type}")
    bm.setHostname(HOSTNAME)
    bm.setWarehouseToken(TOKEN)

    if http_path:
        bm.setWarehouse(http_path)
    else:
        bm.setWarehouseConfig(new_warehouse_config)


    bm.setCatalog(catalog_name)
    bm.setSchema(schema_name)

    bm.preWarmTables(tables)

    bm.setConcurrency(concurrency)
    bm.setQueryRepeatCount(query_repetition_count)
    bm.results_cache_enabled = results_cache_enabled

    if os.path.isdir(query_path):
        bm.setQueryFileDir(query_path)
    else:
        bm.setQueryFile(query_path)
    
    metrics_pdf = bm.execute()
    bm.sql_warehouse.close_connection()
    return  metrics_pdf


def run_multiple_benchmarks(warehouse_prefix):
    with ThreadPoolExecutor(max_workers=3) as executor:
        warehouse_types = ["serverless", "pro", "classic"]
        futures = [executor.submit(run_benchmark, warehouse_prefix, warehouse_type) for warehouse_type in warehouse_types]
        wait(futures, return_when=ALL_COMPLETED)
    
    combined_metrics_pdf = pd.DataFrame()
    for future in futures:
        if combined_metrics_pdf.empty:
            combined_metrics_pdf = future.result()
        else:
            combined_metrics_pdf = pd.concat([combined_metrics_pdf, future.result()])

    return combined_metrics_pdf

def run_multiple_benchmarks_size(warehouse_prefix, warehouse_sizes):
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(run_benchmark, warehouse_prefix, warehouse_type, warehouse_size) for warehouse_size in warehouse_sizes]
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
  metrics_pdf = run_benchmark(warehouse_prefix, warehouse_type)

elif benchmark_choice == "multiple-warehouses":
  metrics_pdf = run_multiple_benchmarks(warehouse_prefix)

elif benchmark_choice == "multiple-warehouses-size":
  metrics_pdf = run_multiple_benchmarks_size(warehouse_prefix, warehouse_sizes)

# COMMAND ----------

# MAGIC %md
# MAGIC Below graph shows average duration of all queries in the warehouse history from start to end of benchmark, broken down by warehouses

# COMMAND ----------

display(metrics_pdf)

# COMMAND ----------

import plotly.graph_objects as go

# Group the metrics by 'id' and 'warehouse_name' and calculate the average total_time_ms
grouped_metrics = metrics_pdf.groupby(['id', 'warehouse_name']).mean(numeric_only=True)['total_time_ms'].reset_index()

# Create a stacked bar chart using Plotly
fig = go.Figure()

# Iterate over each unique warehouse_name and add a bar for each warehouse
for warehouse_name in grouped_metrics['warehouse_name'].unique():
    warehouse_data = grouped_metrics[grouped_metrics['warehouse_name'] == warehouse_name]
    fig.add_trace(go.Bar(
        x=warehouse_data['id'],
        y=warehouse_data['total_time_ms'],
        name=warehouse_name
    ))

# Set the layout of the chart
fig.update_layout(
    xaxis_title='ID',
    yaxis_title='Total Time (ms)',
    title='Query Metrics by Warehouse'
)

# Display the chart
fig.show()

# COMMAND ----------


