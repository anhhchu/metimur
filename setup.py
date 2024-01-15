# Databricks notebook source
from beaker.benchmark import Benchmark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up Delta Generator

# COMMAND ----------

# DBTITLE 1,Set up Environment Variables
import string
import random
from datetime import timedelta, datetime
import dbldatagen as dg
from pyspark.sql.types import *
import pyspark.sql.functions as F
import json
import csv
import time
import os
import requests
from dotenv import load_dotenv
from beaker import Benchmark
import logging
from dbldatagen import DataGenerator, fakerText
from faker.providers import internet
import dbldatagen.distributions as dist

load_dotenv()

# COMMAND ----------

# DBTITLE 1,Global variables for Data Generator
# Some global variables used later in workflow
dbutils.widgets.text(name="catalog", defaultValue="hive_metastore", label="catalog")
dbutils.widgets.text(name="database", defaultValue="dbldatagen", label="database")

# Currently setting warehouse name is not available in v0.0.1 of Beaker release, use defaultValue 
dbutils.widgets.text(name="warehouse_name", defaultValue="🧪 Beaker Benchmark Testing Warehouse", label="warehouse_name")

# COMMAND ----------

# Util for creating string lists of varying word lengths  
def get_random_strings_list(list_len, word_len):
  """Returns a list of random strings."""
  return list(map(lambda x: ''.join(random.choices(string.ascii_uppercase + string.digits, k=word_len)), range(list_len)))

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
database = dbutils.widgets.get("database")
warehouse_name = dbutils.widgets.get("warehouse_name")

# COMMAND ----------

# USE HIVE METASTORE
if catalog == "hive_metastore":
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{database}")
  spark.sql(f"USE {catalog}.{database}")

# USE UNITY CATALOG
elif catalog != "samples":
  spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
  spark.sql(f"USE catalog {catalog}")
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{database}")
  spark.sql(f"USE {catalog}.{database}")

print(f"Use default {catalog}.{database}")

# COMMAND ----------

# DBTITLE 1,Import Table Schema Function
def import_schema(schema_dir):
    """
    Given a directory, this function imports all schema files in csv
    format and returns a dictionary mapping the table name (in the
    format 'catalog.db.table') to the table schema.
    
    Parameters:
    schema_dir (str): the path of the directory containing schema files.
    
    Returns:
    (dict): a dictionary mapping table name to table schema, where the
    table schema is a list of dictionaries, each representing a column
    in the table and its properties.
    """
    table_schemas = {}

    for filename in os.listdir(schema_dir):
      # for each table in schema_dir, import table_schema
      if filename.endswith('.csv'):
        table_name = filename.split('.')[0]
        table = f"{catalog}.{database}.{table_name}"

        with open(os.path.join(schema_dir, filename), 'r') as csv_file:
          csv_reader = csv.DictReader(csv_file, skipinitialspace=True)
          table_schema = []

          for row in csv_reader:
            # process the row
            row = process_row(row)

            # add the row to the table schema
            table_schema.append(row)

        table_schemas[table] = table_schema

    return table_schemas

# COMMAND ----------

def process_row(row):
    """
    This function preprocesses a row of a table schema. It modifies (in place)
    the row's fields such that each value has the expected data type
  
    Args:
    row (dict): a dictionary representing the current row being processed. The keys
    of the dictionary represent properties of the column in the schema, and the values
    represent the values parsed from the csv file.

    Returns:
    dict: the preprocessed row.
    """
    # remove empty fields and strip whitespaces
    row = {k: v.strip() for k, v in row.items() if v.strip()}

    # convert number values to int/float
    for key in ['minValue', 'maxValue', 'step', 'percentNulls']:
        if key in row:
            row[key] = float(row[key])
    
    if row.get('uniqueValues'):
        row['uniqueValues'] = int(row['uniqueValues'])

    # process random and omit fields
    row['random'] = row.get('random', 'False').lower() == 'true'
    row['omit'] = row.get('omit', 'False').lower() == 'true'

    # split the baseColumn to list
    if row.get('baseColumn'):
        row['baseColumn'] = [val.strip() for val in row['baseColumn'].split(',')]

    # convert distribution column to function
    if row.get('distribution'):
        row['distribution'] = eval(row['distribution'])

    # convert text column to function
    if row.get('text'):
      if row['text'].startswith('fakerText'):
        row['text'] = eval(row['text'])
      else:
        print('text field not valid')
 
    if row.get('values'):
        row['values'] = [val.strip() for val in row['values'].split(',')]

    if row.get('weights'):
        row['weights'] = [int(val.strip()) for val in row['weights'].split(',')]

    # convert begin and end from string to datetime
    if row.get('begin') or row.get('end'):
        row['data_range'] = dg.DateRange(row.get('begin'), row.get('end'), row.get('interval'))     

    # drop unused keys
    row.pop('list_len', None)
    row.pop('word_len', None)
    row.pop('masked', None)
    row.pop('comments', None)
    row.pop('begin', None)
    row.pop('end', None)
    row.pop('interval', None)

    return row


def is_valid_date(date_string):
    try:
        datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        return True
    except ValueError:
        return False

# COMMAND ----------

# DBTITLE 1,Generate Delta tables Function
def generate_delta_table(rows, table, table_schema, delta_path):
    """Create a delta table with mockup data from database with specified number of rows

    Args:
        rows (int): The number of rows in the table
        table (str): The name of the delta table database.delta_table
        table_schema (list): A nested list of dictionaries representing the schema for the table
        delta_path (str): The path to the storage location for delta files

    Returns:
        None
    """

    # Create a DataGenerator instance
    df_spec = dg.DataGenerator(spark, name=table, rows=rows, random=True, randomSeed=42)

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

    # Save the dataframe to Delta
    print(f"---Save table to Delta---")
    start_time = time.perf_counter()

    delta_options = {
        'mergeSchema': True,
        'overwriteSchema': True,
    }
        
    df.write.format("delta").mode("overwrite").options(**delta_options).saveAsTable(table)
    end_time = time.perf_counter()
    print(f"Write in {round(end_time - start_time, 2)} seconds---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up Beaker

# COMMAND ----------

# DBTITLE 1,Global variables for Beaker
hostname = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

def get_warehouse(warehouse_name):
  sql_warehouse_url = f"https://{hostname}/api/2.0/sql/warehouses"
  response = requests.get(sql_warehouse_url, headers={"Authorization": f"Bearer {token}"})
  
  if response.status_code == 200:
    for warehouse in response.json()['warehouses']:
      if warehouse['name'] == warehouse_name:
        return(warehouse['id'])
  else:
    print(f"Error: {response.json()['error_code']}, {response.json()['message']}")

# COMMAND ----------

# DBTITLE 1,Beaker benchmark Function
def create_benchmark(
      name="Beaker Benchmark Test",
      query_file_dir=None,
      query_file=None,
      concurrency=1,
      query_repeat_count=1,
      warehouse_http_path=None,
      catalog="hive_metastore",
      schema="default",
      new_warehouse_config=None,
      results_cache_enabled=False,
      db_hostname = hostname,
      token = token 
):
    """
    Create and configure a new Beaker Benchmark object.

    Args:
        name (str, optional): Name to assign to the Benchmark object. Defaults to "beaker_benchmark_test".
        query_file (str): Path to a query file to be used by Benchmark.
        catalog (str): Name of the catalog to be used by Benchmark.
        tables (List[str]): List of table names to pre-warm in Benchmark.
        hostname (str): Name of the host running the warehouse being benchmarked.
        token (str): The warehouse token to access the warehouse specified by hostname.
        concurrency (int, optional): Number of concurrent queries to run. Defaults to 5.
        http_path (Optional[str], optional): The http path to the current warehouse. Required if new_warehouse_config is None. Defaults to None.
        new_warehouse_config (Optional[dict], optional): Dictionary containing configuration information for a new warehouse to be created. Required if http_path is None. Defaults to None.

    Returns:
        Benchmark: A configured Benchmark object.
    """
    benchmark = Benchmark()
    benchmark.setHostname(hostname=hostname)
    benchmark.setWarehouseToken(token=token)
    benchmark.setCatalog(catalog=catalog)

    if new_warehouse_config:
        # Either specify config for a new warehouse
        benchmark.setWarehouseConfig(new_warehouse_config)
    else:
        # Or specify the http_path to the current warehouse
        benchmark.setWarehouse(http_path=http_path)

    # Set concurrency option
    benchmark.setConcurrency(concurrency)
    # Set catalog and query information
    benchmark.setQueryFileDir(query_file_dir=query_file_dir)
    benchmark.setQueryFile(query_file=query_file)
    # benchmark.preWarmTables(tables=tables)
    return benchmark

# COMMAND ----------

# DBTITLE 1,Get Query History from the warehouse
def get_query_history(warehouse_id, workspace_url, pat):
  """
  Retrieves the Query History for a given workspace and Data Warehouse.

  Parameters:
  -----------
  warehouse_id (str): The ID of the Data Warehouse for which to retrieve the Query History.
  workspaceName (str): The workspace URL where the Data Warehouse is located.
  pat(str): The Personal Access Token (PAT) to access the Databricks API.

  Returns:
  --------
  :obj:`requests.Response`
  A Response object containing the results of the Query History API call.
  """
  ## Put together request 

  request_string = {
      "filter_by": {
      #   "query_start_time_range": {
      #   "end_time_ms": end_ts_ms,
      #   "start_time_ms": start_ts_ms
      # },
      "statuses": [
          "FINISHED", "CANCELED"
      ],
      "warehouse_ids": warehouse_id
      },
      "include_metrics": "true",
      "max_results": "1000"
  }

  ## Convert dict to json
  v = json.dumps(request_string)

  uri = f"https://{workspace_url}/api/2.0/sql/history/queries"
  headers_auth = {"Authorization":f"Bearer {pat}"}

  #### Get Query History Results from API
  res = requests.get(uri, data=v, headers=headers_auth)
  return res


# COMMAND ----------

# DBTITLE 1,Generate queries metrics view from query history
from collections.abc import MutableMapping
import pandas as pd

def _flatten_dict(d: MutableMapping, sep: str= '.') -> MutableMapping:
    [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
    return flat_dict
  
def get_query_metrics(response, view_name = "demo_query"):
  end_res = response.json()['res']
  metrics_res = []
  for res_dict in end_res:
    flatten = _flatten_dict(res_dict)
    metrics_res.append(flatten)

  df = spark.createDataFrame( metrics_res)
  df.createOrReplaceTempView(f"{view_name}_hist_view")
  print(f"""Completed creating query metrics from query history, query temp view for details \n
        select * from {view_name}_hist_view""" )

# COMMAND ----------

# DBTITLE 1,Teardown Function
def teardown(delta_path, database):
  """
  Remove all data stored in the delta_path directory and delete the specified database, including all tables and metadata.

  Parameters:
  delta_path (str): The path to the Delta tables that you want to delete.
  database (str): The name of the database you want to delete.

  Returns:
  None
  """
  dbutils.fs.rm(delta_path, recurse=True)
  spark.sql(f"drop schema if exists {database} cascade")


# COMMAND ----------

print("Set Up Completed")
