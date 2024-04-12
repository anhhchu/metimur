from pyspark.sql.functions import col, split, from_json, schema_of_json, to_date
from pyspark.sql.types import StringType, IntegerType, LongType, TimestampType, BooleanType, ArrayType, MapType, StructType


def flatten_map(df, fields):
  for field in df.schema.fields:
    if isinstance(field.dataType, MapType) and field.name in fields:
      keys = [i[0] for i in df.select(F.map_keys(field.name)).distinct().collect()][1]
      change_list = [(k, IntegerType()) for k in keys]
      key_value = [F.col(field.name).getItem(k).alias(k) for k in keys]
      df = df.select("*", *key_value).drop(field.name)
      # Loop through the change_list and modify the column datatype
      for col_name, datatype in change_list:
        df = df.withColumn(col_name, col(col_name).cast(datatype))
  return df

def flatten_struct(df):
  for field in df.schema.fields:
      if isinstance(field.dataType, StructType):
          for child in field.dataType:
              df = df.withColumn(field.name + '_' + child.name, F.col(field.name + '.' + child.name))
          df = df.drop(field.name)
  return df