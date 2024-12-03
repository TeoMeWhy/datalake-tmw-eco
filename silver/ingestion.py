# Databricks notebook source
import sys

sys.path.insert(0, "../lib")

import utils

catalog = 'silver'
database = dbutils.widgets.get("database")
table = dbutils.widgets.get("table")

tablename = f"{catalog}.{database}.{table}"

# COMMAND ----------

query = utils.import_query(f"points/{table}.sql")

(spark.sql(query)
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(tablename))
