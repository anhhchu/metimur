# Databricks notebook source
from pyspark.sql.functions import date_format, collect_set, count_distinct, min, max, col, count, round

def _extract_df_schema(df, table_name):
  print(f"-- Extract schema for table {table_name}")
  rows = df.count()
  # summary = df.summary()

  fields = []
  for column, data_type in df.dtypes:
    distinct_count = df.select(count_distinct(column)).first()[0]
    field = {"colName": column, "colType": data_type, "uniqueValues": distinct_count }

    if data_type in ("string", "bool"):
      if distinct_count > 2000:
        field["template"] = "\w"
      else:
        values_dict = df.groupBy(column).agg(round(count("*")*100 / df.count()).alias("weights")).toPandas().to_dict()
        field["values"] = list(values_dict[column].values())
        field["weights"] = list(values_dict["weights"].values())

    elif data_type in ("timestamp"):
      field["begin"] = df.select(min(column)).first()[0].strftime("%Y-%m-%d %H:%M:%S")
      field["end"] = df.select(max(column)).first()[0].strftime("%Y-%m-%d %H:%M:%S")
    
    elif data_type in ("date"):
      field["begin"] = df.select(min(column)).first()[0].strftime("%Y-%m-%d")
      field["end"] = df.select(max(column)).first()[0].strftime("%Y-%m-%d")

    else:
      min_value = df.agg(min(column)).first()[0]
      max_value = df.agg(max(column)).first()[0]
      if data_type.startswith("decimal"):
        min_value = float(min_value)
        max_value = float(max_value)
        
      field["minValue"] = min_value
      field["maxValue"] = max_value

    fields.append(field)

  table_schema = {"table_name": table_name, "rows": rows, "fields": fields } 
  return table_schema

### TEST THE FUNCTION
# df = spark.table(f"samples.tpch.orders")
# table_schema = _extract_df_schema(df, "orders")
# table_schema

# COMMAND ----------

def extract_table_schemas(catalog_name, schema_name):
  # Get the list of table names in the catalog.schema
  table_names = spark.catalog.listTables(f"{catalog_name}.{schema_name}")
  table_names = [table.name for table in table_names]

  # Extract schema and row count for each table
  table_schemas = []
  for table_name in table_names:
    df = spark.table(f"{catalog_name}.{schema_name}.{table_name}")
    table_schema = _extract_df_schema(df, table_name)
    table_schemas.append(table_schema)
  return table_schemas

### TEST THE FUNCTION
# table_schemas = extract_table_schemas("samples", "nyctaxi")
# table_schemas

# COMMAND ----------

import os
import shutil
import json

def write_json_schemas(table_schemas, output_folder):
  
  if os.path.exists(output_folder):
    shutil.rmtree(output_folder)

  os.mkdir(output_folder)
  # Write the table schemas to JSON files
  for schema_json in table_schemas:
    table_name = schema_json["table_name"]
    file_path = f"{output_folder}/{table_name}.json"
    with open(file_path, "w") as file:
        json.dump(schema_json, file)


# COMMAND ----------

### MAIN
table_schemas = extract_table_schemas("samples", "tpch")
write_json_schemas(table_schemas, "../schemas/tpch")

# COMMAND ----------


