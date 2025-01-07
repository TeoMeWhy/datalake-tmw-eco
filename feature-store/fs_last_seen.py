# Databricks notebook source
import sys
from databricks.feature_engineering import FeatureEngineeringClient

sys.path.insert(0,'../lib')

import utils

fe = FeatureEngineeringClient()

# COMMAND ----------

query = """
SELECT dt_ref, id_cliente
FROM feature_store.points.user_life
WHERE dt_ref = (SELECT MAX(dt_ref) FROM feature_store.points.user_life) 
"""

df = spark.sql(query)

tables = ["feature_store.points.user_life",
          "feature_store.points.user_d7",
          "feature_store.points.user_d14",
          "feature_store.points.user_d28"]

fs_lookups = [utils.create_feature_lookup(spark, i, ["dt_ref", 'id_cliente']) for i in tables]

fs_last_seen_spark = fe.create_training_set(df=df,
                                   feature_lookups=fs_lookups,
                                   label=None,
                                   exclude_columns=['qtde_ativacao'])

fs_last_seen = fs_last_seen_spark.load_df()

(fs_last_seen.write
             .format("delta")
             .mode("overwrite")
             .saveAsTable("feature_store.points.user_last_seen"))
