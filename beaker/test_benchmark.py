# Use reload_ext to reload the changes in other files
%reload_ext autoreload
%autoreload 2

import os
import time
import re
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
import threading

from beaker.sqlwarehouseutils import SQLWarehouseUtils

from beaker.benchmark import Benchmark

dbutils.library.restartPython()

query_file_dir = "../queries/tpch"
query_file = "../queries/tpch/2.sql"

hostname =  spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

catalog = 'samples'
database = 'tpch'
# warehouse_name = '🧪 Beaker Benchmark Testing Warehouse'
warehouse_name = 'ahc_test_warehouse'
warehouseSize = 'X-Small'

def get_warehouse(warehouse_name):
  sql_warehouse_url = f"https://{hostname}/api/2.0/sql/warehouses"
  response = requests.get(sql_warehouse_url, headers={"Authorization": f"Bearer {token}"})
  
  if response.status_code == 200:
    for warehouse in response.json()['warehouses']:
      if warehouse['name'] == warehouse_name:
        return(warehouse['id'])
  else:
    print(f"Error: {response.json()['error_code']}, {response.json()['message']}")

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
        "name": warehouse_name,
        "type": "warehouse",
        "runtime": "latest",
        "size": warehouseSize,
        "min_num_clusters": 1,
        "max_num_clusters": 1,
        "enable_photon": True,
    }


benchmark = Benchmark(name="tpch",
      query_file_dir=None,
      query_file=None,
      concurrency=1,
      query_repeat_count=1,
      warehouse_http_path=http_path,
      catalog=catalog,
      schema=database,
      new_warehouse_config=new_warehouse_config,
      results_cache_enabled=True,
      db_hostname = hostname,
      token = token )

benchmark.setCatalog(catalog)
benchmark.setSchema(database)
benchmark.setQueryFileDir(query_file_dir)

metrics = benchmark.execute()
