import os
import math
from dataclasses import dataclass
from databricks.sdk.runtime import *

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

def check_tables_already_exist(spark, catalog: str, schema: str) -> bool:
    _TABLE_NAMES = []
    if catalog == "tpcds":
        _TABLE_NAMES = _TPCDS_TABLE_NAMES
    elif catalog == "tpch":
        _TABLE_NAMES = _TPCH_TABLE_NAMES
    
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
    # USE HIVE METASTORE
    if catalog == "hive_metastore":
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        spark.sql(f"USE {catalog}.{schema}")

    # USE UNITY CATALOG
    elif catalog != "samples":
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark.sql(f"USE catalog {catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        spark.sql(f"USE {catalog}.{schema}")

    print(f"Data will be saved at {catalog}.{schema}")


############### Set up widgets #############

# widgets in format (dbutils type, args)
_WIDGETS = [
    ("dropdown", ("Benchmarks", "TPCH", ["TPCH", "TPCDS", "BYOD"])),
    ("dropdown", ("Benchmark Choice", "one-warehouse", ["one-warehouse", "multiple-warehouses"])),
    ("text", ("Catalog Name", "hive_metastore")),
    ("text", ("Schema Name", "tpch")),
    ("dropdown", ("Scale Factors", "1", ["1", "10", "100", "1000"])),
    ("text", ("Concurrency", "50")),
    ("dropdown", ("Query Repetition Count", "30", [str(x) for x in range(1, 101)])),
    ("text", ("Warehouse Name", "Benchmarking Warehouse")),
    ("dropdown", ("Warehouse Type", "serverless", ["serverless", "pro", "classic"])),
    (
        "dropdown",
        (
            "Warehouse Size",
            "Small",
            [
                "2X-Small",
                "X-Small",
                "Small",
                "Medium",
                "Large",
                "X-Large",
                "2X-Large",
                "3X-Large",
                "4X-Large",
            ],
        ),
    ),
    ("dropdown", ("Max Clusters", "2", [str(x) for x in range(1, 41)])),
    ("dropdown", ("Channel", "Current", ["Preview", "Current"])),
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
        else:
            raise TypeError(f"{widget_type} type is not supported.")

def create_widgets_benchmark(dbutils):
    dbutils.widgets.removeAll()

    for widget_type, args in _WIDGETS:
        if args[0] != "Scale Factors":
            if widget_type == "text":
                dbutils.widgets.text(*args)
            elif widget_type == "dropdown":
                dbutils.widgets.dropdown(*args)
            else:
                raise TypeError(f"{widget_type} type is not supported.")


def get_widget_values(dbutils):
    widgets_dict = {args[0]: dbutils.widgets.get(args[0]) for _, args in _WIDGETS if args[0] != "Scale Factors"}
    widgets_cleaned = {k.lower().replace(" ", "_"): v for k, v in widgets_dict.items()}

    return {k: _convert_to_int_safe(v) for k, v in widgets_cleaned.items()}




############### Set up constants #############

@dataclass
class Constants:
    ############### Variables dependant upon widgets parameters ##############

    # Name of the catalog to write data to
    catalog_name: str

    # Name of the schema to write data to
    schema_name: str

    # benchmark option
    benchmarks: str

    # benchmark option
    benchmark_choice: str

    # Number of GBs of data to write
    scale_factors: int

    # Name of the warehouse
    warehouse_name: str

    # Type of the warehouse
    warehouse_type: str

    # Size of the warehouse cluster
    warehouse_size: str

    # Warehouse channel name
    channel: str

    # Number of concurrent threads
    concurrency: int

    # Number of times to duplicate the benchmarking run
    query_repetition_count: int

    # Maximum number of clusters
    max_clusters: int

    ############### Variables independent of user parameters #############
    # Name of the job
    job_name = f"[AUTOMATED] Create and run serverless quickstart"

    # Dynamic variables that are used to create downstream variables
    current_user_email = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .userName()
        .get()
    )

    _cwd = os.getcwd().replace("/Workspace", "")

    # User-specific parameters, which are used to create directories and cluster single-access-mode
    current_user_name = (
        current_user_email.replace(".", "_").replace("-", "_").split("@")[0]
    )

    # Base directory where all data and queries will be written
    root_directory = f"dbfs:/Serverless_Benchmarking"

    # Additional subdirectories within the above root_directory
    data_path = os.path.join(root_directory, "data")

    # Location of scripts and queries
    script_path = os.path.join("/Workspace" + _cwd, "scripts")
    query_path = os.path.join("/Workspace" + _cwd, "queries")

    # Location of the spark-sql-perf jar, which is used to create TPC-DS data and queries
    jar_path = os.path.join(script_path, "jars/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar")

    # Location of the init script, which is responsible for installing the above jar and other prerequisites
    init_script_path = os.path.join(script_path, "-install.sh")

    # Location of the dist whl for beaker
    beaker_whl_path = os.path.join(script_path, "beaker-0.0.5-py3-none-any.whl")

    # Location of the notebook that creates
    # datagen_notebook_path = os.path.join(
    #     _cwd, "notebooks/tpc_datagen"
    # )

    # Location of the notebook that runs TPC-DS queries against written data using the beaker library
    run_benchmark_notebook_path = os.path.join(
        _cwd, "notebooks/run_benchmark"
    )

    # Name of the current databricks host
    host = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/"

    def _validate_concurrency_will_utilize_cluster(self):
        required_number_of_clusters = math.ceil(self.concurrency / 10)
        # if self.max_clusters > required_number_of_clusters:

        # print(
        #     "Warning:\n"
        #     "\tFor optimal performance, we recommend using 1 cluster per 10 levels of concurrency. Your currrent\n"
        #     "\tconfiguration will underutilize the warehouse and a cheaper configuration should exhibit the same performance.\n"
        #     f"\tPlease try using {required_number_of_clusters} clusters instead."
        # )
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
                self._cwd, "notebooks/tpc_datagen"
            )

        # Name of the schema that data and benchmarking metrics will be written to
        if self.benchmarks == "TPCDS" or self.benchmarks == "TPCH":
            self.catalog_name = self.benchmarks.lower()
            self.schema_name: str = (
                f"{self.benchmarks.lower().rstrip('_')}_sf{self.scale_factors}_delta"
            )
        # BRING YOUR OWN DATA BENCHMARK 
        elif self.benchmarks == "BYOD":
            self.catalog_name = "serverless_benchmark"
            # Validate schema_name       
            assert (self.schema_name != ""), "Specify the schema_name for BYOD (bring your own data) option!"
            self.schema_name = self.schema_name

        # Set up catalog and schema
        set_up_catalog(spark, self.catalog_name, self.schema_name)

        # Add schema to data path
        self.data_path = os.path.join(self.data_path, self.schema_name)
        self.query_path = os.path.join(self.query_path, self.benchmarks.lower())

        # Determine if TPC tables already exist
        self.tables_already_exist = check_tables_already_exist(spark, self.catalog_name, self.schema_name)

        # Param validations/warnings
        self.max_clusters = self._validate_concurrency_will_utilize_cluster()

        # Set warehouse name
        self.warehouse_name = f"{self.current_user_name} {self.warehouse_name} {self.warehouse_size}"




