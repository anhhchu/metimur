# Databricks notebook source
# MAGIC %md
# MAGIC # Instruction
# MAGIC This notebook simplifies the process of generating TPC (TPCH & TPCDS) industry data at different scale factor or generating synthetic data based on a provided table schemas. Additionally, it enables the execution of query benchmarks on Databricks SQL warehouses with different settings, such as warehouse types, warehouse sizes, and concurrency levels.
# MAGIC
# MAGIC ## Getting Started
# MAGIC 1. Set Up: Run Each cell under the "Set up" section manually to set up parameters. You will also be prompted to specify additional variables based on your choices.
# MAGIC 2. Parameters Update: Update the parameters based on your requirements or keep the default values to observe the functionality.
# MAGIC 3. Executing the Notebook: After making the necessary changes, you can click "Run" or "Run All" to execute the entire notebook with the updated parameters.
# MAGIC 4. Workflow Job Creation: A workflow job will be created with two tasks: **data generation** and **benchmarking**. The URL of the workflow job can be found under the "Run Workflow" section.
# MAGIC
# MAGIC ## Parameters
# MAGIC 1. Benchmarks - TPCH, TPCDS, BYOD (bring your own data):
# MAGIC
# MAGIC * All Data will be generated in the `serverless_benchmark` Unity Catalog
# MAGIC
# MAGIC * For **TPCH** and **TPCDS** option:
# MAGIC   * Optimization: You will be prompted to optimize the data with partitioning and Zorder or not.
# MAGIC
# MAGIC   * Database: By default, TPC Data will be saved under the `tpch_sf{scale_factor}_delta` or `tpcds_sf{scale_factor}_delta` database if partitioned. If not partitioned, it will be saved under `tpch_sf{scale_factor}_delta_nopartitions` or `tpcds_sf{scale_factor}_delta_nopartitions`.
# MAGIC   
# MAGIC   * **Note**: To avoid re-generate these industry benchmarks data, after data is generated, all users in the workspace will be able to query the tables and run benchmark queries on them. If the schemas and tables already exist, the Generate Data task will be skipped
# MAGIC
# MAGIC * **BYOD** option: 
# MAGIC   * Upload your user-defined schema file to the **schemas** folder. Follow the example in **schemas/tpch**
# MAGIC
# MAGIC   * Specify the database name (**schema_name**) and the path to the schema (**schema_path**) in the "Prepare Catalog and Schema" section. Other users won't be able to query the data under your user-defined database.
# MAGIC
# MAGIC 2. Scale Factors:
# MAGIC
# MAGIC Choose the scale factor (in GB) to determine the size of the generated data.
# MAGIC The job will allocate the appropriate number of workers based on the selected scale factor.
# MAGIC
# MAGIC | Scale factor (GB)          | DBSQL recommended size                   |Workers|
# MAGIC | -------------------------- | -----------------------------------------|------ |
# MAGIC | 1                          | Small (32-cores, 4)                      |  4    |
# MAGIC | 10 to 100                  | Medium (64-cores, 8)                     |  8    |
# MAGIC | 1,000 (1TB)                | Large (128-cores, 16 )                   |  16   | 
# MAGIC | 10,000 (10TB)              | X-Large (256-cores, 32)                  |  32   | 
# MAGIC
# MAGIC 3. Benchmark Choice:
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
# MAGIC 4. Query Path: Specify the path to the query file or directory containing the benchmark queries.
# MAGIC * For **TPCH** benchmark, default Query Path is `queries/tpch`. 
# MAGIC * For **TPCDS** benchmark, default Query Path is `queries/tpcds`
# MAGIC * For **BYOD** benchmark, upload the queries to a separate folder under **queries** directory
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
# MAGIC #Setup

# COMMAND ----------

# %pip install -r requirements.txt -q
# # Upgrade databricks-sdk to new version
%pip install --upgrade databricks-sdk -q
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./notebooks/DatabricksClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Parameters

# COMMAND ----------

HOSTNAME = spark.conf.get('spark.databricks.workspaceUrl')
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
catalog_name = "serverless_benchmark"

# COMMAND ----------

# DBTITLE 1,Import Constants
from constants import *

# COMMAND ----------

create_widgets(dbutils)

# COMMAND ----------

# DBTITLE 1,Pull Variables from Notebook Widgets
constants = Constants(
  **get_widget_values(dbutils)
)

print(constants)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data Location

# COMMAND ----------

# DBTITLE 1,Set up Catalog and Schema
def set_up_catalog(spark, catalog:str, schema:str):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `account users`")
    spark.sql(f"GRANT CREATE SCHEMA ON CATALOG {catalog} TO `account users`")
    spark.sql(f"USE catalog {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {schema} TO `account users`")
    if constants.benchmarks != "BYOD":
        # provide permission to query data in tpch and tpcds data, but not on your generated data
        spark.sql(f"GRANT SELECT ON SCHEMA {schema} TO `account users`")
        spark.sql(f"GRANT CREATE TABLE ON SCHEMA {schema} TO `account users`")

    spark.sql(f"USE {catalog}.{schema}")
    print(f"Data will be saved at {catalog}.{schema}")

# COMMAND ----------

# Name of the schema that data and benchmarking metrics will be written to
if constants.benchmarks in ("TPCDS", "TPCH"):
  partition_tables = input("Do you want to optimize TPC data with partitioning and Zorder (true or false)? ", )
  if partition_tables.lower() == "false":
    schema_name = f"{constants.benchmarks.lower()}_sf{constants.scale_factors}_delta_nopartitions"
  else:
    schema_name = f"{constants.benchmarks.lower()}_sf{constants.scale_factors}_delta"
  print(f"TPC Data Schema Name: {catalog_name}.{schema_name}")


# COMMAND ----------

if constants.benchmarks == "BYOD":
  schema_name = input("Provide Schema Name for your generated data: ")
  _schema_path = input("Provide the relative path to your table schema folder (i.e. schemas/tpch): ")
  schema_path = os.path.join(constants._cwd, _schema_path)
  print(f"Custom Data Schema Name: {catalog_name}.{schema_name}")
  print("Custom Data Table Schema Path: ", schema_path)

# COMMAND ----------

set_up_catalog(spark, catalog_name, schema_name)

# COMMAND ----------

# DBTITLE 1,Check if tables exist
# With TPCH and TPCDS, we want to avoid re-generate data
tables_already_exist = False
if constants.benchmarks != "BYOD":
  tables_already_exist = check_tables_already_exist(spark, constants.benchmarks, catalog_name, schema_name)

if tables_already_exist:
  print("Tables already exist, skipping data generation")
else:
  print("Tables do not exist, generating data")

# COMMAND ----------

# MAGIC %md
# MAGIC # Workflow

# COMMAND ----------

# DBTITLE 1,Run Workflow
# Step 1: create the client
client = DatabricksClient(HOSTNAME, TOKEN, constants)

# Step 2: create a job to generate data and runs benchmarks
job_id = client.create_job().job_id

print(job_id)

# COMMAND ----------

# Step 3: Run the job
run_id = client.run_job(job_id).run_id

# Step 4: monitor the job run until completion
url = f"{constants.host.replace('www.','')}#job/{job_id}/run/{run_id}"
print(f"\nA benchmarking job was created at the following url:\n\t{url}\n")
print(f"It will save data at {catalog_name}.{schema_name}")
print(
    "The job may take several hours depending upon data size, so please check back when it's complete.\n"
)

# COMMAND ----------


