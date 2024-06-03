# Databricks notebook source
from pyspark.sql.functions import col, map_keys
from pyspark.sql.types import IntegerType, MapType, StructType
import json
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED


def flatten_map(df, fields):
    """Flatten spark columns of type MapType

    Args:
        df: spark dataframe
        fields: columns to flatten

    Returns:
        df: flattened spark dataframe
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, MapType) and field.name in fields:
            keys = [i[0] for i in df.select(map_keys(field.name)).distinct().collect()][1]
            change_list = [(k, IntegerType()) for k in keys]
            key_value = [col(field.name).getItem(k).alias(k) for k in keys]
            df = df.select("*", *key_value).drop(field.name)
            # Loop through the change_list and modify the column datatype
            for col_name, datatype in change_list:
                df = df.withColumn(col_name, col(col_name).cast(datatype))
    return df


def flatten_struct(df):
    """flatten spark columns of type StructType

    Args:
        df: spark dataframe

    Returns:
        df: spark dataframe
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            for child in field.dataType:
                df = df.withColumn(
                    field.name + "_" + child.name, col(field.name + "." + child.name)
                )
            df = df.drop(field.name)
    return df


def get_comments_from_json(file_path):
    """Get comments from a json file

    Args:
        file_path: path to the json file

    Returns:
        comments: a dictionary of column names and comments
    """

    with open(file_path, "r") as f:
        comments = json.load(f)
    return comments

def _add_comments(table, col, comment):
    alter_sql = f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT {comment};"
    spark.sql(alter_sql)

def create_table_from_df(df, 
                        spark, 
                        catalog_name,
                        schema_name,
                        table_name,
                        comments_file_path='./lakeview_dashboard_gen/column_comments.json', 
                        select_cols=None,
                        overwrite=False):
    """Create a delta table from a spark dataframe and add comments to delta table
    """

    # select columns if specified
    if select_cols:
        df = df.select(select_cols)
    
    # save the dataframe as a table
    print(f"Write the dataframe into {catalog_name}.{schema_name}.{table_name}")
    spark.sql(f"USE catalog {catalog_name};")
    spark.sql(f"USE schema {schema_name};")
    table_exists = spark.catalog.tableExists(f"{catalog_name}.{schema_name}.{table_name}")

    if overwrite:
        (
        df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )
    else:
        (df.write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table_name))

    ## Add comment to each column on newly created table
    if not table_exists:
        print("Add comments to table columns")
        comments = get_comments_from_json(comments_file_path)
        alter_sql = f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} "
        alter_statements = []
        for col in df.columns:
            alter_statement = f"ALTER COLUMN {col} COMMENT '{comments.get(col)}'"
            alter_statements.append(alter_statement)
        alter_sql += ", ".join(alter_statements)
        spark.sql(alter_sql)
