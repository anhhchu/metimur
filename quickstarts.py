# Databricks notebook source
# MAGIC %md 
# MAGIC # Intro
# MAGIC This quickstart notebook provides a convenient way to execute queries concurrently using [Databricks SQL Connector](https://docs.databricks.com/en/dev-tools/python-sql-connector.html) on existing data, and easily benchmark the duration of each query on Serverless, Pro, and Serverless warehouses
# MAGIC
# MAGIC ### Parameters
# MAGIC
# MAGIC * benchmarks: (please ignore)
# MAGIC
# MAGIC * benchmark_choice: This parameter allows you to choose between running the benchmark on a single warehouse ("one-warehouse") or multiple warehouses ("multiple-warehouses"). The default warehouse specification (for `one-warehouse` option) can be chosen from below. If you choose `multiple-warehouses` option, you will run benchmark on serverless, classic, pro warehouse with the same size
# MAGIC
# MAGIC   * warehouse_prefix: This parameter specifies the name prefix of the warehouse. When running the benchmark, the warehouse size and type will be attached to the warehouse_name before spinning up warehouse
# MAGIC
# MAGIC   * warehouse_type: This parameter allows you to select the type of warehouse for the benchmark. The available options are "serverless", "pro", and "classic".
# MAGIC
# MAGIC   * warehouse_size: This parameter determines the size of the warehouse. You can choose from different predefined sizes such as "2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", and "4X-Large".
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
# MAGIC * query_repetition_count: This parameter determines the number of times each query in the benchmark will be executed.
# MAGIC
# MAGIC Specify the concurrency level, and cluster size:
# MAGIC
# MAGIC * concurrency: This parameter sets the level of concurrency, indicating how many queries can be executed simultaneously.
# MAGIC
# MAGIC * max_clusters: This parameter specifies the maximum number of clusters that the warehouse can be scaled up to. We recommend 1 cluster for 10 concurrent queries (maximum 25 clusters)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Set up

# COMMAND ----------

# MAGIC %pip install -r requirements.txt -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Import utils functions
from utils.func import *
hostname = spark.conf.get('spark.databricks.workspaceUrl')
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

# Specify the benchmarking options
dbutils.widgets.dropdown(name="benchmarks", defaultValue="ODBC", choices=["ODBC"])
dbutils.widgets.dropdown(name="benchmark_choice", defaultValue="one-warehouse", choices=["one-warehouse", "multiple-warehouses"])

dbutils.widgets.text(name="warehouse_name", defaultValue="Benchmarking Warehouse", label="warehouse_name")
dbutils.widgets.dropdown(name="warehouse_type", defaultValue="serverless", choices=["serverless", "pro", "classic"], label="warehouse_type")
dbutils.widgets.dropdown(name="warehouse_size", defaultValue="Small", choices=["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"], label="warehouse_size")

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

warehouse_name = dbutils.widgets.get("warehouse_name")
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

# MAGIC %md
# MAGIC # Benchmark

# COMMAND ----------

tables = (spark.sql(f"show tables in {catalog_name}.{schema_name}")
                .where("isTemporary = 'false'")
                .select("tableName")
                .toPandas()["tableName"].tolist()
          )
tables

# COMMAND ----------

from beaker import spark_fixture
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

def run_benchmark(warehouse_name, warehouse_type):

    warehouse_name = f"{warehouse_name} {warehouse_type} {warehouse_size}"
    # Get warehouse id
    warehouse_id = get_warehouse(hostname, token, warehouse_name)

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

    bm = Benchmark()
    bm.setName(f"{benchmarks}_{warehouse_type}")
    bm.setHostname(hostname)
    bm.setWarehouseToken(token)
    bm.setCatalog(catalog_name)
    bm.setSchema(schema_name)

    if http_path:
        bm.setWarehouse(http_path)
    else:
        bm.setWarehouseConfig(new_warehouse_config)

    bm.setConcurrency(concurrency)
    bm.setQueryRepeatCount(query_repetition_count)
    bm.results_cache_enabled = results_cache_enabled

    if os.path.isdir(query_path):
        bm.setQueryFileDir(query_path)
    else:
        bm.setQueryFile(query_path)

    # bm.preWarmTables(tables)
    beaker_metrics, history_metrics = bm.execute()
    metrics_df = spark_fixture.metrics_to_df_view(beaker_metrics, history_metrics, f"{benchmarks}_{warehouse_type}_view" )
    return  metrics_df


def run_multiple_benchmarks(warehouse_name):
    with ThreadPoolExecutor(max_workers=3) as executor:
        warehouse_types = ["serverless", "pro", "classic"]
        futures = [executor.submit(run_benchmark, warehouse_name, warehouse_type) for warehouse_type in warehouse_types]
        wait(futures, return_when=ALL_COMPLETED)
    
    combined_metrics_df = None
    for future in futures:
        if not combined_metrics_df:
            combined_metrics_df = future.result()
        else:
            combined_metrics_df = combined_metrics_df.union(future.result())

    return combined_metrics_df

# COMMAND ----------

if benchmark_choice == "one-warehouse":
  metrics = run_benchmark(warehouse_name, warehouse_type)

elif benchmark_choice == "multiple-warehouses":
  metrics = run_multiple_benchmarks(warehouse_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Below graph shows average duration of all queries in the warehouse history from start to end of benchmark, broken down by warehouses

# COMMAND ----------

metrics.display()

# COMMAND ----------


