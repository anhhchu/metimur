# Databricks notebook source
# MAGIC %pip install -r requirements.txt

# COMMAND ----------

from dbldatagen import DataGenerator, fakerText
from faker.providers import internet
import dbldatagen.distributions as dist


shuffle_partitions_requested = 8
partitions_requested = 8
data_rows = 10000

# partition parameters etc.
spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

my_word_list = [
'danish','cheesecake','sugar',
'Lollipop','wafer','Gummies',
'sesame','Jelly','beans',
'pie','bar','Ice','oat' ]

fakerDataspec = (DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
            .withColumn("name", percentNulls=0.1, text=fakerText("name") )
            .withColumn("phone", percentNulls=0.1, text=fakerText("phone_number") )
            .withColumn("address", text=fakerText("address"))
            .withColumn("company_email", text=eval('fakerText("ascii_company_email")') )
            .withColumn("all_email", text=fakerText("ascii_email") )
            .withColumn("free_email", text=fakerText("ascii_free_email") )
            .withColumn("safe_email", text=fakerText("ascii_safe_email") )
            .withColumn("ip_address", text=fakerText("ipv4_private" ))
            .withColumn("country", text=fakerText("country" ))
            .withColumn("state", text=fakerText("state" ))
            .withColumn("city", text=fakerText("city" ))
            .withColumn("zipcode", text=fakerText("zipcode" ))
            .withColumn("faker_text", text=fakerText("sentence", ext_word_list=my_word_list))
            .withColumn("decimal_val","decimal(10,2)",minValue=1,maxValue=100000,random=True,distribution=eval('dist.Gamma(1.0, 2.0)'))
            .withColumn("distinct", "string", values=['a', 'b', 'c'], random=True, weights=[7, 2, 1])
            .withColumn("beta_dist","int",minValue=1,maxValue=100,random=True,distribution=eval('dist.Beta(1.0, 2.0)'))
            .withColumn("gamma_dist","int",minValue=1,maxValue=100,random=True,distribution=eval('dist.Gamma(1.0, 2.0)'))
            .withColumn("normal_dist","int",minValue=1,maxValue=100,random=True,distribution=eval('dist.Normal(1.0, 2.0)'))
            )
dfFakerOnly = fakerDataspec.build()

# dfFakerOnly.write.format("delta").mode("overwrite").save("/tmp/test-output")

# COMMAND ----------

display(dfFakerOnly)

# COMMAND ----------

# https://databrickslabs.github.io/dbldatagen/public_docs/generating_from_existing_data.html
import dbldatagen as dg

dfSource = spark.read.format("delta").load("dbfs:/anhhoang.chu/hive_metastore.anh_dbldatagen.customers")

analyzer = dg.DataAnalyzer(sparkSession=spark, df=dfSource)

generatedCode = analyzer.scriptDataGeneratorFromData()
# code =  dg.DataAnalyzer.scriptDataGeneratorFromSchema(dfSource.schema)

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.functions as F

# clear cache so that if we run multiple times to check performance,
# we're not relying on cache
spark.catalog.clearCache()

UNIQUE_PLANS = 20
PLAN_MIN_VALUE = 100

shuffle_partitions_requested = 8
partitions_requested = 1
data_rows = UNIQUE_PLANS # we'll generate one row for each plan

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)


plan_dataspec = (
    dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
    .withColumn("plan_id","int", minValue=PLAN_MIN_VALUE, uniqueValues=UNIQUE_PLANS)
    # use plan_id as root value
    .withColumn("plan_name", prefix="plan", baseColumn="plan_id")

    # note default step is 1 so you must specify a step for small number ranges,
    .withColumn("cost_per_mb", "decimal(5,3)", minValue=0.005, maxValue=0.050,
                step=0.005, random=True)
    .withColumn("cost_per_message", "decimal(5,3)", minValue=0.001, maxValue=0.02,
                step=0.001, random=True)
    .withColumn("cost_per_minute", "decimal(5,3)", minValue=0.001, maxValue=0.01,
                step=0.001, random=True)

    # we're modelling long distance and international prices simplistically -
    # each is a multiplier thats applied to base rate
    .withColumn("ld_multiplier", "decimal(5,3)", minValue=1.5, maxValue=3, step=0.05,
                random=True, distribution="normal", omit=True)
    .withColumn("ld_cost_per_minute", "decimal(5,3)",
                expr="cost_per_minute * ld_multiplier",
                baseColumns=['cost_per_minute', 'ld_multiplier'])
    .withColumn("intl_multiplier", "decimal(5,3)", minValue=2, maxValue=4, step=0.05,
                random=True,  distribution="normal", omit=True)
    .withColumn("intl_cost_per_minute", "decimal(5,3)",
                expr="cost_per_minute * intl_multiplier",
                baseColumns=['cost_per_minute', 'intl_multiplier'])
            )

df_plans = plan_dataspec.build().cache()

display(df_plans)

# COMMAND ----------

import re

MARGIN_PATTERN= re.compile(r"\s*\|")  # margin detection pattern for stripMargin
def stripMargin(s):
  """  strip margin removes leading space in multi line string before '|' """
  return "\n".join(re.split(MARGIN_PATTERN, s))

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.functions as F

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)

UNIQUE_CUSTOMERS = 50000
CUSTOMER_MIN_VALUE = 1000
DEVICE_MIN_VALUE = 1000000000
SUBSCRIBER_NUM_MIN_VALUE = 1000000000

spark.catalog.clearCache()  # clear cache so that if we run multiple times to check
                            # performance, we're not relying on cache
shuffle_partitions_requested = 8
partitions_requested = 8
data_rows = UNIQUE_CUSTOMERS

customer_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
            .withColumn("customer_id","decimal(10)", minValue=CUSTOMER_MIN_VALUE,
                        uniqueValues=UNIQUE_CUSTOMERS)
            .withColumn("customer_name", template=r"\\w \\w|\\w a. \\w")

            # use the following for a simple sequence
            #.withColumn("device_id","decimal(10)", minValue=DEVICE_MIN_VALUE,
            #              uniqueValues=UNIQUE_CUSTOMERS)

            .withColumn("device_id","decimal(10)",  minValue=DEVICE_MIN_VALUE,
                        baseColumn="customer_id", baseColumnType="hash")

            .withColumn("phone_number","decimal(10)",  minValue=SUBSCRIBER_NUM_MIN_VALUE,
                        baseColumn=["customer_id", "customer_name"], baseColumnType="hash")

            # for email, we'll just use the formatted phone number
            .withColumn("email","string",  format="subscriber_%s@myoperator.com",
                        baseColumn="phone_number")
            .withColumn("plan", "int", minValue=PLAN_MIN_VALUE, uniqueValues=UNIQUE_PLANS,
                        random=True)
            )

df_customers = (customer_dataspec.build()
                .dropDuplicates(["device_id"])
                .dropDuplicates(["phone_number"])
                .orderBy("customer_id")
                .cache()
               )

effective_customers = df_customers.count()

print(stripMargin(
  f"""revised customers : {df_customers.count()},
   |   unique customers: {df_customers.select(F.countDistinct('customer_id')).take(1)[0][0]},
   |   unique device ids: {df_customers.select(F.countDistinct('device_id')).take(1)[0][0]},
   |   unique phone numbers: {df_customers.select(F.countDistinct('phone_number')).take(1)[0][0]}""")
     )

display(df_customers)

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.functions as F

AVG_EVENTS_PER_CUSTOMER = 50

spark.catalog.clearCache()
shuffle_partitions_requested = 8
partitions_requested = 8
NUM_DAYS=31
MB_100 = 100 * 1000 * 1000
K_1 = 1000
data_rows = AVG_EVENTS_PER_CUSTOMER * UNIQUE_CUSTOMERS * NUM_DAYS

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)


# use random seed method of 'hash_fieldname' for better spread - default in later builds
events_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested,
                   randomSeed=42, randomSeedMethod="hash_fieldname")
             # use same logic as per customers dataset to ensure matching keys
             # but make them random
            .withColumn("device_id_base","decimal(10)", minValue=CUSTOMER_MIN_VALUE,
                        uniqueValues=UNIQUE_CUSTOMERS,
                        random=True, omit=True)
            .withColumn("device_id","decimal(10)",  minValue=DEVICE_MIN_VALUE,
                        baseColumn="device_id_base", baseColumnType="hash")

            # use specific random seed to get better spread of values
            .withColumn("event_type","string",
                        values=[ "sms", "internet", "local call", "ld call", "intl call" ],
                        weights=[50, 50, 20, 10, 5 ], random=True)

            # use Gamma distribution for skew towards short calls
            .withColumn("base_minutes","decimal(7,2)",
                        minValue=1.0, maxValue=100.0, step=0.1,
                        # distribution=dg.distributions.Gamma(shape=1.5, scale=2.0),
                        distribution = "gamma(1.5,2.0)",
                        random=True, omit=True)

            # use Gamma distribution for skew towards short transfers
            .withColumn("base_bytes_transferred","decimal(12)",
                        minValue=K_1, maxValue=MB_100,
                        distribution=dg.distributions.Gamma(shape=0.75, scale=2.0),
                        random=True, omit=True)

            .withColumn("minutes", "decimal(7,2)",
                        baseColumn=["event_type", "base_minutes"],
                        expr= """
                              case when event_type in ("local call", "ld call", "intl call")
                                  then base_minutes
                                  else 0
                              end
                               """)
            .withColumn("bytes_transferred", "decimal(12)",
                        baseColumn=["event_type", "base_bytes_transferred"],
                        expr= """
                              case when event_type = "internet"
                                   then base_bytes_transferred
                                   else 0
                              end
                               """)

            .withColumn("event_ts", "timestamp",
                         data_range=dg.DateRange("2020-07-01 00:00:00",
                                                 "2020-07-31 11:59:59",
                                                 "seconds=1"),
                        random=True)

            )

df_events = events_dataspec.build()

display(df_events)

# COMMAND ----------

