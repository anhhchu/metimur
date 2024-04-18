# Databricks notebook source
from pyspark.sql.functions import col, map_keys
from pyspark.sql.types import IntegerType, MapType, StructType
import json


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


def create_view_from_df(df, 
                        spark, 
                        catalog_name,
                        schema_name,
                        comments_file_path='./lakeview_dashboard_gen/column_comments.json', 
                        select_cols=None):
    """Create a temporary view from a spark dataframe

    Args:
        df: spark dataframe
        view_name: name of the view
        select_cols: columns to select, if None, select all columns

    Returns:
        None
    """

    # select columns if specified
    if select_cols:
        df = df.select(select_cols)
    
    # save the dataframe as a table
    print(f"Write the dataframe into {catalog_name}.{schema_name}.metrics_processed")
    spark.sql(f"USE catalog {catalog_name};")
    spark.sql(f"USE schema {schema_name};")
    (df.write
       .mode("overwrite")
       .saveAsTable("metrics_processed"))
    
    # create view sql script with comments
    comments = get_comments_from_json(comments_file_path)
    view_sql = f"CREATE OR REPLACE VIEW metimur_metrics (\n"
    for col in df.columns:
        if col in comments:
            view_sql += f"  {col} COMMENT '{comments[col]}',\n"
        else:
            view_sql += f"  {col},\n"
    view_sql = view_sql[:-2] + "\n) AS SELECT * FROM metrics_processed;"

    print(f"Creating a view named metimur_metric from the metrics and selected columns ...")
    spark.sql(view_sql)
