# Databricks notebook source
# MAGIC %pip install -r requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Import utils functions
from utils import *

# COMMAND ----------

hostname = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate data
# MAGIC
# MAGIC Generate data for bring your own data

# COMMAND ----------

schemaPath = 

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
