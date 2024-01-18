# Use reload_ext to reload the changes in other files
%reload_ext autoreload
%autoreload 2

from sqlwarehouseutils import SQLWarehouseUtils

hostname =  spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

catalog = 'samples'
database = 'tpch'
# warehouseName = '🧪 Beaker Benchmark Testing Warehouse'
warehouseName = 'ahc_serverless'
warehouseSize = 'X-Small'

new_warehouse_config = {
    "name": warehouseName,
    "type": "warehouse",
    "warehouse": "serverless",
    "runtime": "latest",
    "size": warehouseSize,
    "min_num_clusters": 1,
    "max_num_clusters": 1,
    "enable_photon": True,
}

# warehouse_utils = SQLWarehouseUtils()
# warehouse_utils.setToken(token=token)
# warehouse_utils.setHostname(hostname=hostname)
# warehouse_utils.setCatalog(catalog)
# warehouse_utils.setSchema(database)
# warehouse_utils.setEnableResultCaching(results_cache_enabled=True)
# warehouse_id = warehouse_utils.launch_warehouse(new_warehouse_config)

# warehouse_id

import requests
id = "b4dff13711bd485d"

response = requests.post(f"https://{hostname}/api/2.0/sql/warehouses/{id}/start", 
                         headers={"Authorization": f"Bearer {token}"})

print(response.status_code == 200)

response = requests.get(f"https://{hostname}/api/2.0/sql/warehouses/{id}", 
                         headers={"Authorization": f"Bearer {token}"})

response.json()["state"]

