# Databricks notebook source
import sys

sys.path.insert(0, "../lib")

import utils

# COMMAND ----------

catalog = 'gold'
database = 'teomewhy'
table = dbutils.widgets.get("table")
tablename = f"{catalog}.{database}.{table}"

query = utils.import_query(f"{table}.sql")

df = spark.sql(query)

(df.write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(tablename))
