# Databricks Serverless SQL Accelerator
![brick](./assets/brick.png)
## About the Accelerator

* Automating data generation and query benchmarking
* Removing dependencies on customer data access
* Suggesting optimization techniques
* Benchmark queries performance in Databricks Serverless SQL

This accelerator utilizes DatabricksLabs [dbldatagen](https://github.com/databrickslabs/dbldatagen) package for DataFrame generation, and [Beaker](https://github.com/goodwillpunning/beaker) package for query benchmarking 


## Requirements
* Databricks workspace with Serverless and Unity Catalog enabled

## Quickstarts

The **quickstart** notebook provides a convenient way to execute queries concurrently using [Databricks SQL Connector](https://docs.databricks.com/en/dev-tools/python-sql-connector.html)on existing data, and easily benchmark the duration of each query on Serverless, Pro, and Serverless warehouses

<span style="background-color: yellow">**Note**: You should have existing data available in the workspace to proceed. If you don't have available data, the default data used in the notebook is `samples.tpch` data along with tpch sample queries in `queries` folder. If you want to use TPCH and TPCDS data with different scale factor, or generate your own data with defined schema, go to **Advanced** section.</span>

### Set up
Clone this repo to your Databricks Workspace. Refer to [Databricks Repo](https://docs.databricks.com/en/repos/repos-setup.html) for instruction

1. Upload your query file to `queries` folder, refer to `tpch` folder for sample queries. 
2. Go to `quickstarts` notebook
3. Specify parameters
4. Click `Run/Run All`

### Output


## Advanced (in progress)

The **advanced** notebook provides a convenient way to:  
1. Generate TPCH and TPCDS data (if not exists) Or generate your own sample data with available schemas
2. Execute queries concurrently using [Databricks SQL Connector](https://docs.databricks.com/en/dev-tools/python-sql-connector.html) on existing data, and easily benchmark the duration of each query on Serverless, Pro, and Serverless warehouses





