# Databricks notebook source
# MAGIC %pip install -r requirements.txt -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Constants
from constants import *

# COMMAND ----------

# DBTITLE 1,Add Widgets to Notebook
create_widgets(dbutils)

# COMMAND ----------

# DBTITLE 1,Pull Variables from Notebook Widgets
constants = Constants(
  **get_widget_values(dbutils)
)

# COMMAND ----------

print("Environment variables set:")
constants

# COMMAND ----------

# DBTITLE 1,Run Benchmark
from utils.run import run

run(spark, dbutils, constants)

# COMMAND ----------


