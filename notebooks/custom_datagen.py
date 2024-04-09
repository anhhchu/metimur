# Databricks notebook source
# MAGIC %md
# MAGIC # Set Up

# COMMAND ----------

# MAGIC %pip install -r requirements.txt -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys
import os

# COMMAND ----------

hostname = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text(name="catalog_name", defaultValue="serverless_benchmark")
dbutils.widgets.text(name="schema_name", defaultValue="tpch_datagen")
dbutils.widgets.text(name="schema_path", defaultValue="../schemas/tpch")
  
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
schema_path = dbutils.widgets.get("schema_path")

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate data
# MAGIC
# MAGIC Generate data for bring your own data (BYOD)

# COMMAND ----------

import dbldatagen as dg
import time

def generate_dataframe(rows, table, table_schema):

    # Create a DataGenerator instance
    df_spec = dg.DataGenerator(spark, name=table, rows=rows, partition=4, random=True, randomSeed=42)

    # # Use the withIdOutput() method to retain the id field in the output data
    # df_spec.withIdOutput()

    # Loop through the table schema and call df_spec.withColumn() with each field
    for field in table_schema:
      df_spec.withColumn(**field)

    # Build the dataframe
    start_time = time.perf_counter()
    df = df_spec.build()
    count = df.count()
    end_time = time.perf_counter()
    build_time = round(end_time - start_time, 2)
    print(f"---Dataframe of {count} rows, built in {build_time} seconds---")
    return df

def generate_delta_table(df, table_name):
    # Save the dataframe to Delta
    print(f"---Save table {table_name} to Delta---")
    start_time = time.perf_counter()

    delta_options = {
        'mergeSchema': True,
        'overwriteSchema': True,
    }
        
    df.write.format("delta").mode("overwrite").options(**delta_options).saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")
    end_time = time.perf_counter()
    print(f"Write in {round(end_time - start_time, 2)} seconds---")

# COMMAND ----------

def get_files_from_dir(schema_path, type="json"):
  # Get a list of all files in the schema_path directory
  file_list = os.listdir(schema_path)

  # Filter out the JSON files
  files = [file for file in file_list if file.endswith(type)]
  return files

# COMMAND ----------

import json
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

def process_file(file_name):
  print("fileName: ", file_name)
  with open(os.path.join(schema_path, file_name), "r") as file:
    json_data = json.load(file)

  # Extract the table name, rows, and fields from the parsed JSON data
  table_name = json_data["table_name"]
  rows = json_data["rows"]
  fields = json_data["fields"]

  fields = [{key: value for key, value in field.items() if value is not None and value != []} for field in fields]

  # Call the generate_delta_table function
  df = generate_dataframe(rows, table_name, fields)
  generate_delta_table(df, table_name)

# COMMAND ----------

files = get_files_from_dir(schema_path)
with ThreadPoolExecutor(max_workers=len(files)) as executor:
  futures = [executor.submit(process_file, file_name) for file_name in files]
  wait(futures, return_when=ALL_COMPLETED)
