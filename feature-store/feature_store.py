# Databricks notebook source
from tqdm import tqdm
import sys

sys.path.insert(0, "../lib")

import utils

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# COMMAND ----------

catalog = 'feature_store'
database = 'points'
table = 'user_life'
table_name = f'{catalog}.{database}.{table}'
dt_start = '2024-04-01'
dt_stop = '2024-12-01'
monthly = False
query_name = 'points_template'
primary_keys = ['dt_ref', 'id_cliente']
partition_by = 'dt_ref'
days = 10000

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
