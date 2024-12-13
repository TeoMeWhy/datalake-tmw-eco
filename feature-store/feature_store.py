# Databricks notebook source
from tqdm import tqdm
import sys

sys.path.insert(0, "../lib")

import utils

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# COMMAND ----------

catalog = 'feature_store'
database = dbutils.widgets.get("database")
table = dbutils.widgets.get("table")
query_name = dbutils.widgets.get("query_name")
primary_keys = dbutils.widgets.get("primary_keys").split(",")
partition_by = dbutils.widgets.get("partition_by")
dt_start = dbutils.widgets.get("dt_start")
dt_stop = dbutils.widgets.get("dt_stop")
monthly = dbutils.widgets.get("monthly") == 'true'

days = dbutils.widgets.get("days")

table_name = f'{catalog}.{database}.{table}'

dates = utils.range_date(dt_start, dt_stop, monthly)
query = utils.import_query(f"{query_name}.sql")

# COMMAND ----------

if not utils.tables_exists(spark, catalog, database, table):
    df = spark.sql(query.format(dt_ref=dates.pop(0), days=days))
    fe.create_table(df=df,
                name=table_name,
                primary_keys=primary_keys,
                partition_columns=partition_by )


for d in tqdm(dates):
    df = spark.sql(query.format(dt_ref=d, days=days))
    fe.write_table(df=df, name=table_name, mode='merge')
