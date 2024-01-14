# Databricks notebook source
# MAGIC %md
# MAGIC #Databricks Labs Data Generator (dbldatagen)
# MAGIC The Databricks Labs Data Generator project provides a convenient way to generate large volumes of synthetic data from within a Databricks notebook (or a regular Spark application). 
# MAGIC
# MAGIC By defining a data generation spec, either in conjunction with an existing schema or through creating a schema on the fly, you can control how synthetic data is generated.
# MAGIC
# MAGIC As the data generator generates a Spark data frame, it is simple to create a Delta table, or a view to use with Scala, Python, R, or SQL.
# MAGIC
# MAGIC **Resources**: 
# MAGIC * [GitHub Repo](https://github.com/databrickslabs/dbldatagen)
# MAGIC * [Documentation](https://databrickslabs.github.io/dbldatagen/public_docs/index.html)

# COMMAND ----------

# MAGIC %md
# MAGIC # Installation
# MAGIC 1. Install from PyPi with `%pip install` in notebook
# MAGIC 2. Install from Databricks Labs repo source
# MAGIC   `%pip install git+https://github.com/databrickslabs/dbldatagen@current`
# MAGIC 3. Install a specific release: 
# MAGIC   `%pip install https://github.com/databrickslabs/dbldatagen/releases/download/v021/dbldatagen-0.2.1-py3-none-any.whl`
# MAGIC 4. Install through Databricks workspace and attach library to cluster

# COMMAND ----------

# DBTITLE 1,Installing via PyPi
# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting Started
# MAGIC 1. Initialize a datagen object. Parameters: 
# MAGIC   * `name`: Name of the datagen object, default None
# MAGIC   * `rows`: The number of rows to be generated, default 100000
# MAGIC   * `partitions`: Number of partitions, default `spark.sparkContext.defaultParallelism`
# MAGIC   * `random`: whether data is generated randomly or not, default is False. If random = True, specify `randomSeed` and `randomSeedMethod`
# MAGIC 2. Add columns to a datagen object using `withColumn`
# MAGIC   * At the bare minimum, you need to specify the column name, and the data type
# MAGIC   * You can use string format or pyspark data types format for a column's data type
# MAGIC   * Notice that if nothing else is specified, string columns will take integer values, and the data will monotonically increase, which is not very interesting
# MAGIC 3. Use `build` method to generate the dataframe:
# MAGIC   * Until the build method is invoked, the data generation specification is in initialization mode.
# MAGIC   * Once build has been invoked, the data generation instance holds state about the data set generated.
# MAGIC
# MAGIC **Notice the use of `withIdOutput()` to generate monotonically increasing `id` column**
# MAGIC   

# COMMAND ----------

import dbldatagen as dg 
from pyspark.sql.types import *

# COMMAND ----------

sample_dataspec = (
    dg.DataGenerator(name="sample-datagen", rows=100, partitions=4, random=True, randomSeed=42)
      .withIdOutput()
      .withColumn("stringCol", "string")
      .withColumn("stringCol", StringType())
      .withColumn("integerCol", "integer")
      .withColumn("integerCol2", IntegerType())
      .withColumn("booleanCol", "boolean")
      .withColumn("booleanCol2", BooleanType())
      .withColumn("doubleCol", "double")
      .withColumn("dateCol", "date")
      .withColumn("timestampCol", "timestamp")
)

sample_df = sample_dataspec.build()
display(sample_df)

# COMMAND ----------

sample_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Generate Numeric Data
# MAGIC * `uniqueValues`:  control the unique values for a column
# MAGIC * `minValue`, `maxValue` and `step`: generate values in a range
# MAGIC * `distribution`: Controls the statistical distribution of random values when the column is generated randomly. Accepts the values `normal`, or a Distribution object instance such as `Beta`, `Gamma`, `Exponential`. This is helpful to introduce skew to your data
# MAGIC * generate from existing values by using `baseColumn` and `expr`

# COMMAND ----------

numeric_dataspec = (
    dg.DataGenerator(name="numeric-datagen", rows=100, partitions=4, random=True, randomSeed=42)
      .withIdOutput()
      .withColumn("hash_id", LongType(), minValue=0x10000000000,
                uniqueValues=100,  baseColumnType="hash")
      .withColumn("num1", IntegerType(), minValue=10000, 
                maxValue=2000000, random=True)
      .withColumn("num2", IntegerType(), uniqueValues=20, random=True)
      .withColumn("num3", IntegerType(), baseColumn=["num2"], expr="num2*10")
      .withColumn("val1", DoubleType(), minValue=0.0, maxValue=9999.95, step=0.01, random=True, percentNulls = 0.01)
      .withColumn("normalVal", "double", minValue=0.0, maxValue=9999.95, distribution=dg.distributions.Normal(2000, 2.5), random=True)
      .withColumn("gammaVal", "int", minValue=1, maxValue=100, distribution=dg.distributions.Gamma(1.0, 2.0), random=True)
)

numeric_df = numeric_dataspec.build()
display(numeric_df)

# COMMAND ----------

# Number of unique values in num2
from pyspark.sql.functions import countDistinct
numeric_df.select(countDistinct("num2")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Text Data
# MAGIC 1. `uniqueValues`: control the number of unique values
# MAGIC 2. `values`: list of discrete values
# MAGIC 3. `weights`: list of discrete weights for the colummn. should be integer values. Normally, specify with `values` option
# MAGIC 4. `template`: allows specification of templated text based on specified options. [Reference](https://databrickslabs.github.io/dbldatagen/public_docs/textdata.html)
# MAGIC 5.  generate text from existing values by using `baseColumn` and `expr`
# MAGIC   * add a prefix or suffix: `.withcolumn("local_device", "string", prefix="device", baseColumn="local_device_id")`
# MAGIC   * use custom sql expression: `.withColumn("event_date", "date", expr="to_date(tag_ts)", baseColumn="tag_ts")`
# MAGIC 6. (experimental) Use `text` option and `fakerText` to generate text data with faker library. Make sure you install faker library with `%pip install faker`
# MAGIC
# MAGIC **Notice the use of `omit=True` below to exclude the base column in the final dataframe**
# MAGIC

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

from dbldatagen import fakerText 
from faker.providers import internet

my_word_list = ['databricks', 'lakehouse', 'awesome', 'deltalake', 'lakehouseiq', 'lakehouseai', 'dbldatagen', 'finssa' ]

text_dataspec = (
    dg.DataGenerator(name="text-datagen", rows=100, partitions=4, random=True, randomSeed=42)
      .withIdOutput()
      .withColumn("code", StringType(), uniqueValues=10, template=r'\\w', random=True)
      .withColumn("code2", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1])
      .withColumn("local_device_id", "int", maxValue=20, omit=True, random=True) # baseColumn
      .withColumn("local_device", "string", prefix="device", baseColumn="local_device_id")
      .withColumn("local_device_suffix", "string", suffix="device", baseColumn="local_device_id")
      .withColumn("line", "string", values=['line 1', 'line 2', 'line 3', 'line 4', 'line 5', 'line 6'], random=True, omit=True) # baseColumn
      .withColumn("deviceKey", "string", expr = "concat('/', line, '/', local_device)", baseColumn=["line", "local_device"])
      .withColumn("template_email", template=r'\w.\w@\w.com|\w@\w.com.u\s')
      .withColumn("template_ip_addr", template=r'\n.\n.\n.\n')
      .withColumn("template_phone", template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd')
      .withColumn("name", percentNulls=0.1, text=fakerText("name") )
      .withColumn("address", text=fakerText("address" ))
      .withColumn("email", text=fakerText("ascii_company_email") )
      .withColumn("ip_address", text=fakerText("ipv4_private" ))
      .withColumn("faker_text", text=fakerText("sentence", ext_word_list=my_word_list))
      )

text_df = text_dataspec.build()
display(text_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Date and Timestamp
# MAGIC * `begin`, `end` and `interval`: for date and timestamp columns, format `'%Y-%m-%d %H:%M:%S'`
# MAGIC * `dg.DateRange(begin, end, interval=None, datetime_format='%Y-%m-%d %H:%M:%S')`: use Date Range class to generate datetime data
# MAGIC * `uniqueValues`: if a number of unique values is specified, these will be generated starting from the start date time and incremented according to the interval. So if a date ranges is specified for a year with an interval of 7 days but only 10 unique values, the max value will be the starting date or time + 10 weeks
# MAGIC * generate from existing values by using `baseColumn` and `expr`
# MAGIC
# MAGIC **Recommend to explicitly specify a start and end date time.**
# MAGIC
# MAGIC [Reference](https://databrickslabs.github.io/dbldatagen/public_docs/DATARANGES.html)
# MAGIC

# COMMAND ----------

date_dataspec = (
    dg.DataGenerator(name="numeric-datagen", rows=100, partitions=4, random=True, randomSeed=42)
      .withIdOutput()
      .withColumn("random_date", "date", uniqueValues=300, random=True)
      .withColumn(
          "purchase_date",
          "date",
          data_range=dg.DateRange("2017-10-01 00:00:00", "2018-10-06 11:55:00", "days=3"),
          random=True,
      )
      .withColumn(
          "return_date",
          "date",
          expr="date_add(purchase_date, cast(floor(rand() * 100 + 1) as int))",
          baseColumn="purchase_date",
      )
      .withColumn(
          "event_time",
          "timestamp",
          data_range=dg.DateRange("2017-10-01 00:00:00", "2018-10-06 11:55:00", "seconds=5"),
          random=True,
      )
      .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00",
            end="2020-12-31 23:59:00",
            interval="1 minute", random=True)
)

date_df = date_dataspec.build()
display(date_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Complex Structured Data
# MAGIC There are several methods for generating nested data:
# MAGIC
# MAGIC * Generate a dataframe and save it as JSON will generate full data set as JSON
# MAGIC * Generate JSON valued fields using SQL functions such as `named_struct` and `to_json`
# MAGIC * Generate Array fields with `structType='array'` and `numFeatures`

# COMMAND ----------

complex_dataspec = (
    dg.DataGenerator(spark, name="complex_date", rows=1000,
                     partitions=8, randomSeedMethod='hash_fieldname')
      .withIdOutput()
      .withColumn("event_type", StringType(),
                values=["activation", "deactivation", "plan change",
                        "telecoms activity", "internet activity", "device error"],
                random=True, omit=True)
      .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00",
                  end="2020-12-31 23:59:00",
                  interval="1 minute", random=True, omit=True)
      # generate nested json data
      .withColumn("event_info",
          StructType([StructField('event_type',StringType()),
                      StructField('event_ts', TimestampType())]),
          expr="named_struct('event_type', event_type, 'event_ts', event_ts)",
          baseColumn=['event_type', 'event_ts'])   
      # Generate array data
      .withColumn("event_array",
          "struct<event_type:string,event_ts:timestamp>",
          structType='array',
          expr="named_struct('event_type', event_type, 'event_ts', event_ts)", 
          baseColumn=['event_type', 'event_ts'],
          numFeatures=(1,2))
)

complex_df = complex_dataspec.build()
display(complex_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Streaming Data
# MAGIC If the `withStreaming` option is used when building the data set, it will use a streaming rate source to generate the data. You can control the streaming rate with the option `rowsPerSecond`. The `rows` parameter will be ignored

# COMMAND ----------

# DBTITLE 1,Write stream to Delta
from datetime import timedelta, datetime
import dbldatagen as dg

interval = timedelta(days=1, hours=1)
start = datetime(2017, 10, 1, 0, 0, 0)
end = datetime(2018, 10, 1, 6, 0, 0)

# row count will be ignored
ds = (dg.DataGenerator(spark, name="association_oss_cell_info", rows=1000, partitions=20)
      .withColumn("site_id", minValue=1, maxValue=20, step=1)
      .withColumn("site_cd", prefix='site', baseColumn='site_id')
      .withColumn("sector_status_desc", "string", minValue=1, maxValue=200, step=1,
                  prefix='status', random=True)
      .withColumn("rand", "float", expr="floor(rand() * 350) * (86400 + 3600)")
      .withColumn("last_sync_dt", "timestamp", begin=start, end=end, interval=interval,
                  random=True)
      .withColumn("sector_technology_desc", values=["GSM", "UMTS", "LTE", "UNKNOWN"],
                  random=True)
      )

df = ds.build(withStreaming=True, options={'rowsPerSecond': 500})
display(df)

# COMMAND ----------

(
  df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/dbldatagen_anh/streamingDemo/checkpoint")
    .start("/tmp/dbldatagen_anh/streamingDemo/data")
)

# COMMAND ----------

# DBTITLE 1,Terminate Stream
# note stopping the stream may produce exceptions - these can be ignored
for x in spark.streams.active:
    try:
        x.stop()
    except RuntimeError:
        pass

# delete data path
dbutils.fs.rm("/tmp/dbldatagen_anh/", recurse=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Generate Synthetic Data from existing schema (experimental)
# MAGIC 1. Use `summarizeToDF()` under `DataAnalyzer` class to determine distribution and skew in data
# MAGIC 2. Use `scriptDataGeneratorFromSchema` or `scriptDataGeneratorFromData` to generate code from existing schema or Spark df
# MAGIC

# COMMAND ----------

source = 'dbfs:/databricks-datasets/retail-org/solutions/silver/sales_orders'
display(dbutils.fs.ls(source))

# COMMAND ----------

dfSource = spark.read.format("delta").load(source)
analyzer = dg.DataAnalyzer(sparkSession=spark, df=dfSource)
display(analyzer.summarizeToDF())

# COMMAND ----------

display(dfSource)

# COMMAND ----------

# DBTITLE 1,Generate from Existing Data
code = analyzer.scriptDataGeneratorFromData()

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.types

# Column definitions are stubs only - modify to generate correct data  
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=1000,
                     random=True,
                     )
    .withColumn('clicked_items', 'array<string>', expr='null', structType='array', numFeatures=(2,6))
    .withColumn('customer_id', 'string', template=r'\\w')
    .withColumn('customer_name', 'string', template=r'\\w')
    .withColumn('number_of_line_items', 'string', template=r'\\w')
    .withColumn('order_datetime', 'string', template=r'\\w')
    .withColumn('order_number', 'bigint', minValue=317568014, maxValue=317572013)
    .withColumn('ordered_products', 'struct<curr:string,id:string,name:string,price:bigint,promotion_info:struct<promo_disc:double,promo_id:bigint,promo_item:string,promo_qty:bigint>,qty:bigint,unit:string>', expr='null', structType='array', numFeatures=(2,6))
    .withColumn('promo_info', 'struct<promo_disc:double,promo_id:bigint,promo_item:string,promo_qty:bigint>', expr='null', structType='array', numFeatures=(2,6))
    .withColumn('order_timestamp', 'timestamp', begin="2020-01-01 01:00:00", end="2020-12-31 23:59:00", interval="1 minute" )
    .withColumn('missing_order_time', 'boolean', expr='id % 2 = 1')
    )

synthetic_df = generation_spec.build()
display(synthetic_df)


# COMMAND ----------

dfSource.schema

# COMMAND ----------

# DBTITLE 1,Generate from Schema
# The schema needs to be in pyspark.sql.types.StructType format
schema = StructType([StructField('clicked_items', ArrayType(ArrayType(StringType(), True), True), True), StructField('customer_id', StringType(), True), StructField('customer_name', StringType(), True), StructField('number_of_line_items', StringType(), True), StructField('order_datetime', StringType(), True), StructField('order_number', LongType(), True), StructField('ordered_products', ArrayType(StructType([StructField('curr', StringType(), True), StructField('id', StringType(), True), StructField('name', StringType(), True), StructField('price', LongType(), True), StructField('promotion_info', StructType([StructField('promo_disc', DoubleType(), True), StructField('promo_id', LongType(), True), StructField('promo_item', StringType(), True), StructField('promo_qty', LongType(), True)]), True), StructField('qty', LongType(), True), StructField('unit', StringType(), True)]), True), True), StructField('promo_info', ArrayType(StructType([StructField('promo_disc', DoubleType(), True), StructField('promo_id', LongType(), True), StructField('promo_item', StringType(), True), StructField('promo_qty', LongType(), True)]), True), True), StructField('order_timestamp', TimestampType(), True), StructField('missing_order_time', BooleanType(), True)])

# COMMAND ----------

code = dg.DataAnalyzer.scriptDataGeneratorFromSchema(schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # Resources

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters for Datagen object
# MAGIC `data_spec = (dg.DataGenerator(**params))`
# MAGIC
# MAGIC * sparkSession: spark Session object to use, default None
# MAGIC * **name**: name of the dataset, 
# MAGIC * **rows**: number of rows to generate
# MAGIC
# MAGIC * seedColumnName: if set, this should be the name of the `seed` or logical `id` column. Defaults to `id`. By default the seed column is named `id`. If you need to use this column name in your generated data, it is recommended that you use a different name for the seed column - for example `_id`.
# MAGIC * startingId: starting value for generated seed column
# MAGIC
# MAGIC * **random**: if set, specifies default value of `random` attribute for all columns where not set
# MAGIC * **randomSeedMethod**: seed method for random numbers - either None, `fixed`, `hash_fieldname`. If the randomSeedMethod value is hash_fieldname, the random seed for each column is computed using a hash function over the field name. `RandomSeedMethod` attribute value is fixed, it will be generated using a random number generator with a designated `randomSeed` unless the `randomSeed` value is -1
# MAGIC * **randomSeed**: seed for random number generator
# MAGIC
# MAGIC * **partitions**: number of partitions to generate, if not provided, uses `spark.sparkContext.defaultParallelism`
# MAGIC * verbose: if `True`, generate verbose output
# MAGIC * batchSize: UDF batch number of rows to pass via Apache Arrow to Pandas UDFs
# MAGIC * debug: if set to True, output debug level of information
# MAGIC
# MAGIC [reference](https://databrickslabs.github.io/dbldatagen/public_docs/reference/api/dbldatagen.data_generator.html?highlight=hash_fieldname)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters for `withColumn` method
# MAGIC [reference](https://databrickslabs.github.io/dbldatagen/public_docs/options_and_features.html)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other capabilities of dbldatagen
# MAGIC 1. [Repeatable Data Gen](https://databrickslabs.github.io/dbldatagen/public_docs/repeatable_data_generation.html): describe how dbldatagen can reproduce data 
# MAGIC 2. [Generate dependent data in multiple tables for join](https://databrickslabs.github.io/dbldatagen/public_docs/multi_table_data.html): generate multiple tables with consistent primary and foreign keys to model join or merge scenarios
# MAGIC 3. [Generate CDC Data](https://databrickslabs.github.io/dbldatagen/public_docs/generating_cdc_data.html): ability to generate a base data set and then apply changes such as updates to existing rows, inserting new rows, or deleting rows

# COMMAND ----------

