# Databricks notebook source
from pyspark.sql.functions import date_format

def _extract_df_schema(df, table_name):
  rows = df.count()

  fields = []
  for column, data_type in df.dtypes:
    distinct_count = None
    values = []
    min_value = None
    max_value = None
    if data_type in ("string", "bool"):
      distinct_values = df.select(column).distinct()
      distinct_count = distinct_values.count()
      if distinct_count < 1000:
        values = [row[column] for row in distinct_values.collect() if row[column] is not None]
    elif data_type in ("timestamp", "date"):
      # Get the minimum value of the column
      min_value = df.agg({column: "min"}).collect()[0][0]
      min_value = min_value.strftime("%Y-%m-%d %H:%M:%S")  # Convert to string format

      # Get the maximum value of the column
      max_value = df.agg({column: "max"}).collect()[0][0]
      max_value = max_value.strftime("%Y-%m-%d %H:%M:%S")  # Convert to string format

    fields.append({"colName": column, "colType": data_type, "uniqueValues": distinct_count, "values": values, "minValue": min_value, "maxValue": max_value })

  table_schema = {"table_name": table_name, "rows": rows, "fields": fields } 
  return table_schema

df = spark.table(f"samples.tpch.customer")
table_schema = _extract_df_schema(df, "customer")

table_schema

# COMMAND ----------

df.summary().display()

# COMMAND ----------

def extract_table_schemas(catalog_name, schema_name):
  # Get the list of table names in the catalog.schema
  table_names = spark.catalog.listTables(f"{catalog_name}.{schema_name}")
  table_names = [table.name for table in table_names]

  # Extract schema and row count for each table
  table_schemas = []
  for table_name in table_names:
    df = spark.table(f"{catalog_name}.{schema_name}.{table_name}")
    table_schema = extract_df_schema(df)
    table_schemas.append(table_schema)
  return table_schemas

table_schemas = extract_schemas("samples", "tpch")

# COMMAND ----------

import os
import shutil
import json

def write_json_schemas(output_folder):
  if os.path.exists(output_folder):
    shutil.rmtree(output_folder)

  os.mkdir(output_folder)
  # Write the table schemas to JSON files
  for schema_json in table_schemas:
    table_name = schema_json["table_name"]
    file_path = f"{output_folder}/{table_name}.json"
    with open(file_path, "w") as file:
        json.dump(schema_json, file)

write_json_schemas("../schemas/tpch")

# COMMAND ----------


