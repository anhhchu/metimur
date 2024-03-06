# To use a Databricks built-in object in a Python module, import it from databricks.sdk.runtime
from databricks.sdk.runtime import *

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
import logging
from dbldatagen import DataGenerator, fakerText
from faker.providers import internet
import dbldatagen.distributions as dist
import pandas as pd
from databricks.sdk import WorkspaceClient
import math


# Util for creating string lists of varying word lengths  
def get_random_strings_list(list_len, word_len):
  """Returns a list of random strings."""
  return list(map(lambda x: ''.join(random.choices(string.ascii_uppercase + string.digits, k=word_len)), range(list_len)))

def import_schema(catalog, schema, schema_path):
    """
    Given a directory, this function imports all schema files in csv
    format and returns a dictionary mapping the table name (in the
    format 'catalog.db.table') to the table schema.
    
    Parameters:
    schema_path (str): the path of the directory containing schema files.
    
    Returns:
    (dict): a dictionary mapping table name to table schema, where the
    table schema is a list of dictionaries, each representing a column
    in the table and its properties.
    """
    table_schemas = {}

    for filename in os.listdir(schema_path):
      # for each table in schema_path, import table_schema
      if filename.endswith('.csv'):
        table_name = filename.split('.')[0]
        table = f"{catalog}.{schema}.{table_name}"

        with open(os.path.join(schema_path, filename), 'r') as csv_file:
          csv_reader = csv.DictReader(csv_file, skipinitialspace=True)
          table_schema = []

          for row in csv_reader:
            # process the row
            row = process_row(row)

            # add the row to the table schema
            table_schema.append(row)

        table_schemas[table] = table_schema

    return table_schemas

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
      

def generate_delta_table(rows, table, table_schema, delta_tokenh):
    """Create a delta table with mockup data from database with specified number of rows

    Args:
        rows (int): The number of rows in the table
        table (str): The name of the delta table database.delta_table
        table_schema (list): A nested list of dictionaries representing the schema for the table
        delta_tokenh (str): The tokenh to the storage location for delta files

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


def get_warehouse(hostname, token, warehouse_name):
  sql_warehouse_url = f"https://{hostname}/api/2.0/sql/warehouses"
  response = requests.get(sql_warehouse_url, headers={"Authorization": f"Bearer {token}"})
  
  if response.status_code == 200:
    for warehouse in response.json()['warehouses']:
      if warehouse['name'] == warehouse_name:
        return(warehouse['id'])
  else:
    print(f"Error: {response.json()['error_code']}, {response.json()['message']}")

def teardown(catalog, database):
  """
  Remove all data stored in the delta_path directory and delete the specified database, including all tables and metadata.

  Parameters:
  delta_path (str): The path to the Delta tables that you want to delete.
  database (str): The name of the database you want to delete.

  Returns:
  None
  """
  spark.sql(f"drop schema if exists {catalog}.{database} cascade")

