import os
import math
from dataclasses import dataclass
from typing import Optional
from databricks.sdk.runtime import *
import logging

############### Set up tables #############

_TPCDS_TABLE_NAMES = {
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
}

_TPCH_TABLE_NAMES = {"customer", "lineitem", "nation", "orders", "part", "region", "supplier", "partsupp"}


def check_tables_already_exist(spark, benchmarks, catalog: str, schema: str) -> bool:
    if catalog == "samples":
        return True
    
    _TABLE_NAMES = []
    if benchmarks == "TPCDS":
        _TABLE_NAMES = _TPCDS_TABLE_NAMES
    elif benchmarks == "TPCH":
        _TABLE_NAMES = _TPCH_TABLE_NAMES
    
    if benchmarks in ("TPCDS", "TPCH"):
        if (spark.sql("show catalogs").where(f"catalog ILIKE '{catalog}'").limit(1).count()> 0):
            if (
                spark.sql(f"show databases in {catalog}")
                .where(f"databaseName ILIKE '{schema}'")
                .limit(1)
                .count()
                > 0
            ):
                tables = set(
                    spark.sql(f"show tables in {catalog}.{schema}")
                    .where("tableName not ILIKE 'benchmark%'")
                    .select("tableName")
                    .toPandas()["tableName"]
                )
                return all(x in tables for x in _TABLE_NAMES)

    return False

def set_up_catalog(spark, catalog:str, schema:str):
    logging.info("Set up catalog and schema")
    # USE HIVE METASTORE
    if catalog == "hive_metastore":
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        spark.sql(f"USE {catalog}.{schema}")

    # USE UNITY CATALOG
    elif catalog != "samples":
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `account users`")
        spark.sql(f"USE catalog {catalog}")
        spark.sql(f"GRANT USE SCHEMA  ON CATALOG {catalog} TO `account users`")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        spark.sql(f"USE {catalog}.{schema}")

    logging.info(f"Data will be saved at {catalog}.{schema}")


############### Set up widgets #############
VALID_WAREHOUSES = ["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"]


WORKERS_SCALE_FACTOR_MAP = {1:4, 10:4, 100:8, 1000:16, 10000:32}

# widgets in format (dbutils type, args)
_WIDGETS_BASE = [
    ("text", ("Warehouse Prefix", "Metimur")),

    ("text", ("Query Path", "queries/tpcds")),
    ("text", ("Concurrency", "1")),

    ("dropdown", ("Benchmark Choice", "one-warehouse", ["one-warehouse", "multiple-warehouses", "multiple-warehouses-size"])),
    
    ("dropdown", ("Warehouse Type", "serverless", ["serverless", "pro", "classic"])),
    ("multiselect", ("Warehouse Size", "Small", VALID_WAREHOUSES)),
    ("dropdown", ("Query Repetition Count", "1", [str(x) for x in range(1, 101)])),

    ("dropdown", ("Max Clusters", "1", [str(x) for x in range(1, 41)])),
    ("dropdown", ("Results Cache Enabled", "False", ["True", "False"])),
]

# Use _WIDGETS in advanced notebook 
_WIDGETS = _WIDGETS_BASE + [
    ("dropdown", ("Benchmarks", "TPCDS", ["TPCH", "TPCDS", "BYOD"])),
    # ("text", ("Catalog Name", "serverless_benchmark")),
    # ("text", ("Schema Name", "")),
    ("dropdown", ("Scale Factors", "1", ["1", "10", "100", "1000"]))
]

# Use _WIDGETS in quickstarts notebook 
_WIDGETS_BENCHMARK = _WIDGETS_BASE + [
    ("text", ("Catalog Name", "samples")),
    ("text", ("Schema Name", "tpch")),
]

def _convert_to_int_safe(s: str):
    try:
        return int(s)
    except ValueError as e:
        if "invalid literal for int()" in str(e):
            return s
        else:
            raise
    except:
        raise


def create_widgets(dbutils):
    dbutils.widgets.removeAll()
    for widget_type, args in _WIDGETS:
        if widget_type == "text":
            dbutils.widgets.text(*args)
        elif widget_type == "dropdown":
            dbutils.widgets.dropdown(*args)
        elif widget_type == "multiselect":
            dbutils.widgets.multiselect(*args)
        else:
            raise TypeError(f"{widget_type} type is not supported.")


def get_widget_values(dbutils):
    widgets_dict = {args[0]: dbutils.widgets.get(args[0]) for _, args in _WIDGETS }
    widgets_cleaned = {k.lower().replace(" ", "_"): v for k, v in widgets_dict.items()}

    return {k: _convert_to_int_safe(v) for k, v in widgets_cleaned.items()}


def create_widgets_benchmark(dbutils):
    """Use in quickstart_db notebook"""
    dbutils.widgets.removeAll()
    for widget_type, args in _WIDGETS_BENCHMARK:
        if args[0] != "Scale Factors" and args[0] != "Schema Path":
            if widget_type == "text":
                dbutils.widgets.text(*args)
            elif widget_type == "dropdown":
                dbutils.widgets.dropdown(*args)
            elif widget_type == "multiselect":
                dbutils.widgets.multiselect(*args)
            else:
                raise TypeError(f"{widget_type} type is not supported.")

def get_widget_values_benchmark(dbutils):
    """Use in quickstart_db notebook"""
    widgets_dict = {args[0]: dbutils.widgets.get(args[0]) for _, args in _WIDGETS_BENCHMARK}
    widgets_cleaned = {k.lower().replace(" ", "_"): v for k, v in widgets_dict.items()}

    return {k: _convert_to_int_safe(v) for k, v in widgets_cleaned.items()}



############### Set up constants #############

@dataclass
class Constants:
    ############### Variables dependant upon widgets parameters ##############

    # Number of times to duplicate the benchmarking run
    query_repetition_count: int

    # Path to query 
    query_path: str

    # Maximum number of clusters
    max_clusters: int

    # Result Cache Enabled
    results_cache_enabled: bool

    benchmark_choice: str

    # Number of concurrent threads
    concurrency: int

    # Prefix of the warehouse
    warehouse_prefix: str

    # Type of the warehouse
    warehouse_type: str

    # Size of the warehouse cluster
    warehouse_size: str

    # # Warehouse channel name
    # channel: str

    # # Name of the catalog to write data to
    # catalog_name: str

    # # Name of the schema to write data to
    # schema_name: str

    # Number of GBs of data to write
    scale_factors: Optional[int] = None

    # # Path to schema 
    # schema_path: Optional[str] = None

    # benchmark option
    benchmarks: Optional[str] = None




    ############### Variables independent of user parameters #############
    # Name of the job
    job_name = f"[AUTOMATED] Metimur Benchmark"

    # Dynamic variables that are used to create downstream variables
    current_user_email = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .userName()
        .get()
    )

    _cwd = os.getcwd() #.replace("/Workspace", "")

    # User-specific parameters, which are used to create directories and cluster single-access-mode
    current_user_name = (
        current_user_email.replace(".", "_").replace("-", "_").split("@")[0]
    )

    # Base directory where TPC data and queries will be written
    root_directory = f"dbfs:/Serverless_Benchmark"

    # Additional subdirectories within the above root_directory
    data_path = os.path.join(root_directory, "data")

    # Location of scripts and queries
    script_path = os.path.join(_cwd, "scripts")

    # Location of the spark-sql-perf jar, which is used to create TPC-DS data and queries
    jar_path = os.path.join(root_directory, "jars/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar")

    # Location of the init script, which is responsible for installing the above jar and other prerequisites
    init_script_path = os.path.join(script_path, "tpc-install.sh")

    # Location of the dist whl for beaker
    beaker_whl_path = os.path.join(script_path, "beaker-0.0.7-py3-none-any.whl")

    # Location of the notebook that creates
    # datagen_notebook_path = os.path.join(
    #     _cwd, "notebooks/tpc_datagen"
    # )

    # Location of the notebook that runs queries against written data using the beaker library
    run_benchmark_notebook_path = os.path.join(
        _cwd, "quickstarts"
    )

    # Name of the current databricks host
    host = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/"

    def _validate_concurrency_will_utilize_cluster(self):
        required_number_of_clusters = math.ceil(self.concurrency / 10)
        print(f"Warning: For optimal performance, we recommend using 1 cluster per 10 levels of concurrency. \n Set maximum number of clusters to {required_number_of_clusters} based on required concurrency")
        return required_number_of_clusters


    def __post_init__(self):

        # Location of the notebook that creates
        if self.benchmarks == "BYOD":
            self.datagen_notebook_path = os.path.join(
                self._cwd, "notebooks/custom_datagen"
            )
        else:
            self.datagen_notebook_path = os.path.join(
                self._cwd, "notebooks/TPC_datagen"
            )


        # # BRING YOUR OWN DATA BENCHMARK 
        # elif self.benchmarks == "BYOD":
        #     self.catalog_name = f"benchmark_{self.current_user_name}"
        #     # Validate schema_name       
        #     assert (self.schema_name != ""), "Specify the schema_name for BYOD (bring your own data) option!"
        #     self.schema_name = self.schema_name

        # Set up catalog and schema
        # set_up_catalog(spark, self.catalog_name, self.schema_name)

        # Add schema to data path
        # self.data_path = os.path.join(self.data_path, self.schema_name)

        if self.query_path:
            self.query_path = os.path.join(self._cwd, self.query_path)
        # if self.schema_path:
        #     self.schema_path = os.path.join(self._cwd, self.schema_path)

        # Convert result cache enabled to boolean
        self.results_cache_enabled = False if self.results_cache_enabled == "False" else True

        # # Determine if TPC tables already exist
        # self.tables_already_exist = check_tables_already_exist(spark, self.benchmarks, self.catalog_name, self.schema_name)

        # Set warehouse prefix
        # Add current_user_name to warehouse to avoid conflict
        self.warehouse_prefix = f"{self.current_user_name} {self.warehouse_prefix}"

        # Param validations/warnings
        self._validate_concurrency_will_utilize_cluster()
        # self.max_clusters = self._validate_concurrency_will_utilize_cluster()

        # Mapping of number of workers to scale factor, format {scale_factors: workers}
        self.workers_scale_factor_map = WORKERS_SCALE_FACTOR_MAP[self.scale_factors]



