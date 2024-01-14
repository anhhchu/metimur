# Databricks notebook source
# MAGIC %md
# MAGIC # Set up
# MAGIC * Clone this notebook
# MAGIC * Update parameters in below cell
# MAGIC * Note: 
# MAGIC   * If using Unity Catalog, specify catalog name and use cluster with UC enabled
# MAGIC   * Currently setting warehouse name is not available in v0.0.1 of Beaker release, 
# MAGIC     * use defaultValue "🧪 Beaker Benchmark Testing Warehouse", 
# MAGIC     * or substitute with an existing warehouse in your workspace

# COMMAND ----------

# MAGIC %pip install -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benchmarking options
# MAGIC
# MAGIC TPC-H: “TPC-H is a decision support benchmark. It consists of a suite of business-oriented ad hoc queries and concurrent data modifications. The queries and the data populating the database have been chosen to have broad industry-wide relevance. This benchmark illustrates decision support systems that examine large volumes of data, execute queries with a high degree of complexity, and give answers to critical business questions.”
# MAGIC
# MAGIC TPC-DS: 
# MAGIC
# MAGIC | Data | Catalog     | Description            |
# MAGIC | ---- | ----------- | ---------------------- |
# MAGIC | tpch | sample.tpch | Databricks sample data |
# MAGIC |      |             |                        |

# COMMAND ----------

# Specify the benchmarking options
dbutils.widgets.dropdown(name="benchmark", defaultValue="tpch", choices=["tpch", "tpch_sf1", "tpch_sf10", "tpch_sf100", "tpch_sf1000","tpcds_sf10", "tpcds_sf100",  "BYOD"])

# Specify name for your warehouse
dbutils.widgets.text(name="warehouse_name", defaultValue="🧪 Beaker Benchmark Testing Warehouse", label="warehouse_name")

#Specify your schema directory and query file location
dbutils.widgets.text(name="schemaPath", defaultValue="", label="schemaPath")
dbutils.widgets.text(name="queryPath", defaultValue="", label="queryPath")

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
else:
  catalog = "serverless_benchmark"
  database = benchmark

dbutils.widgets.text(name="catalog", defaultValue=catalog)
dbutils.widgets.text(name="database", defaultValue=database)

# COMMAND ----------

# MAGIC %run ./setup 
# MAGIC   $catalog=$catalog
# MAGIC   $database=$database
# MAGIC   $warehouse_name=$warehouse_name

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate table

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

tables=["samples.tpch.customer", "samples.tpch.lineitem", "samples.tpch.nation", "samples.tpch.orders", "samples.tpch.part", "samples.tpch.partsupp", "samples.tpch.region", "samples.tpch.supplier"]

# COMMAND ----------

# Get warehouse id again in case cell is rerun
warehouse_id = get_warehouse(warehouse_name)

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
        "type": "warehouse",
        "runtime": "latest",
        "size": warehouseSize,
        "min_num_clusters": 1,
        "max_num_clusters": 1,
        "enable_photon": True,
    }

metrics_view = "tpch"

benchmark = create_benchmark(
    name=metrics_view,
    query_file=queryPath,
    query_file_dir = None,
    concurrency=concurrency,
    query_repeat_count=1,
    warehouse_http_path=None,
    catalog=catalog,
    schema=database,
    new_warehouse_config=new_warehouse_config,
    results_cache_enabled=False
)

metrics = benchmark.execute()

# COMMAND ----------

spark.sql(f"select *, float(query_duration_secs) as query_duration from {metrics_view}_vw;").display()

# COMMAND ----------

# Check Query History metrics
warehouse_id = get_warehouse(warehouse_name)
print("Warehouse id: ", warehouse_id)
response = get_query_history( warehouse_id, hostname, token)
get_query_metrics(response, view_name = metrics_view)

# COMMAND ----------

# MAGIC %md
# MAGIC Below graph shows average duration of all queries in the warehouse (if we run the queries multiple times)

# COMMAND ----------

spark.sql(f"select * from {metrics_view}_hist_view").display()

# COMMAND ----------

spark.sql(
    f"""select v2.id, v1.query_text, 
  v1.`duration`/1000 as duration_sec, 
  v1.`metrics.query_execution_time_ms`/1000 as query_execution_time_sec, 
  v1.`metrics.planning_time_ms`/1000 as planning_time_sec, 
  v1.`metrics.photon_total_time_ms`/1000 as photon_total_time_sec, 
  float(v2. query_duration_secs) as query_duration 
from {metrics_view}_hist_view v1
join {metrics_view}_vw v2
on v1.query_text = v2.query_text
"""
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Teardown
# MAGIC Run at the end of the analysis

# COMMAND ----------

teardown(delta_path=storage_dir, database=database)

# COMMAND ----------



# COMMAND ----------

