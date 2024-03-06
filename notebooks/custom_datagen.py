# Databricks notebook source
# MAGIC %md
# MAGIC # Set Up

# COMMAND ----------

import sys
import os

sys.path.append(os.path.abspath(".."))
_cwd = os.getcwd()
_pwd = os.path.dirname(_cwd)
sys.path.append(_pwd)

# COMMAND ----------

# MAGIC %pip install -r requirements.txt -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Import utils functions
from utils.func import *
# from constants import *

# COMMAND ----------

hostname = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text(name="catalog_name", defaultValue="serverless_benchmark")
dbutils.widgets.text(name="schema_name", defaultValue="demos")
dbutils.widgets.text(name="schema_path", defaultValue="../schemas/demos")
  
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
schema_path = dbutils.widgets.get("schema_path")

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate data
# MAGIC
# MAGIC Generate data for bring your own data (BYOD)

# COMMAND ----------

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
      

def generate_delta_table(rows, table, table_schema):
    """Create a delta table with mockup data from database with specified number of rows

    Args:
        rows (int): The number of rows in the table
        table (str): The name of the delta table database.delta_table
        table_schema (list): A nested list of dictionaries representing the schema for the table

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

# DBTITLE 1,Import schema definition file
table_schemas = import_schema(catalog_name, schema_name, schema_path)
tables = list(table_schemas.keys())
tables

# COMMAND ----------

# DBTITLE 1,Set table rows
output_rows = {}
for table in table_schemas.keys():
  rows = input(f"rows for table {table}")
  output_rows[table] = int(rows)

# COMMAND ----------

table_schemas["serverless_benchmark.demos.events"]

# COMMAND ----------

# DBTITLE 1,Generate Delta Tables
for table in tables:
  sc.setJobDescription(f"Generate table {table}") 
  print(f"**Data generation for {table}**")
  generate_delta_table(output_rows[table], table, table_schemas[table])

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

for table in tables:
  spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS;")
